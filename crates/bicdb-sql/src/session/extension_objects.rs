//! Objects a contrib extension defines: created by `CREATE EXTENSION`,
//! removed by `DROP EXTENSION` (which refuses while table columns use them).
use super::*;
use crate::pg_operators::{
    delete_operator, save_operator, OperatorFunction, OperatorRef, OperatorSchema,
};

impl<'db> SqlSession<'db> {
    /// Creates the objects of a newly created extension.
    pub(crate) fn install_extension_objects(&mut self, name: &str, schema: &str) -> Result<()> {
        if name.eq_ignore_ascii_case("hstore") {
            self.install_hstore(schema)?;
        }
        Ok(())
    }

    /// Removes the objects of an extension being dropped.
    pub(crate) fn remove_extension_objects(&mut self, name: &str, cascade: bool) -> Result<()> {
        if !name.eq_ignore_ascii_case("hstore") {
            return Ok(());
        }
        let Some(user_type) = list_user_types(self.db_ref())?
            .into_iter()
            .find(|user_type| user_type.name == "hstore" && is_hstore_type(&user_type.kind))
        else {
            return Ok(());
        };
        let used = list_schemas(self.db_ref())?.into_iter().any(|schema| {
            schema.columns.iter().any(|column| {
                column
                    .user_type
                    .as_ref()
                    .is_some_and(|column_type| column_type.oid == user_type.oid)
            })
        });
        if used {
            if cascade {
                return Err(SqlError::Unsupported(
                    "DROP EXTENSION hstore CASCADE with dependent columns is not supported"
                        .to_string(),
                ));
            }
            return Err(SqlError::dependent_objects_still_exist(
                "cannot drop extension hstore because other objects depend on it".to_string(),
            ));
        }
        for (operator, left, right, _, _) in crate::hstore::OPERATORS {
            delete_operator(
                self.db_mut()?,
                &OperatorRef {
                    schema: user_type.schema_name.clone(),
                    name: (*operator).to_string(),
                    left: Some((*left).to_string()),
                    right: (*right).to_string(),
                },
            )?;
        }
        self.delete_session_user_type(&user_type.schema_name, &user_type.name)?;
        Ok(())
    }

    fn install_hstore(&mut self, schema: &str) -> Result<()> {
        if load_user_type(self.db_ref(), schema, "hstore")?.is_some() {
            return Ok(());
        }
        let oids = allocate_user_type_oids(self.db_mut()?, 2)?;
        let user_type = UserTypeSchema {
            name: "hstore".to_string(),
            schema_name: schema.to_string(),
            owner: current_user_from_gucs(&self.session_gucs),
            comment: Some("data type for storing sets of (key, value) pairs".to_string()),
            acl_explicit: false,
            oid: oids[0],
            array_oid: oids[1],
            kind: UserTypeKind::Base {
                codec_type: "text".to_string(),
                input: HSTORE_INPUT.to_string(),
                output: "hstore_out".to_string(),
                receive: None,
                send: None,
                internal_length: -1,
                passed_by_value: false,
                alignment: 'i',
                storage: 'x',
                category: 'U',
                preferred: false,
                default_expr: None,
                element_type: None,
                delimiter: ',',
                collatable: false,
            },
        };
        self.save_session_user_type(&user_type)?;
        let owner = current_user_from_gucs(&self.session_gucs);
        for (operator, left, right, function, result) in crate::hstore::OPERATORS {
            let operator = OperatorSchema {
                id: OperatorRef {
                    schema: schema.to_string(),
                    name: (*operator).to_string(),
                    left: Some((*left).to_string()),
                    right: (*right).to_string(),
                },
                left_declaration: Some(qualified_type(schema, left)),
                right_declaration: qualified_type(schema, right),
                function: Some(OperatorFunction {
                    schema: schema.to_string(),
                    name: (*function).to_string(),
                    result: (*result).to_string(),
                }),
                commutator: None,
                negator: None,
                restrict: None,
                join: None,
                hashes: false,
                merges: false,
                owner: Some(owner.clone()),
            };
            save_operator(self.db_mut()?, &operator)?;
        }
        Ok(())
    }
}

/// The input function name that marks the extension's hstore type.
pub(crate) const HSTORE_INPUT: &str = "hstore_in";

pub(crate) fn is_hstore_type(kind: &UserTypeKind) -> bool {
    matches!(kind, UserTypeKind::Base { input, .. } if input == HSTORE_INPUT)
}

fn qualified_type(schema: &str, pg_type: &str) -> String {
    if pg_type.starts_with("hstore") && schema != "public" {
        format!("{schema}.{pg_type}")
    } else {
        pg_type.to_string()
    }
}
