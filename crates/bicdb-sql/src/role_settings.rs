//! Per-role session defaults: `ALTER ROLE name SET parameter TO value` and
//! `ALTER ROLE name RESET parameter | ALL`. A login applies its role's
//! defaults before the client's startup parameters, which take precedence.
use crate::*;

fn setting_value(text: &str) -> String {
    let mut values = Vec::new();
    let (mut depth, mut quote, mut start) = (0usize, None::<char>, 0usize);
    let mut parts = Vec::new();
    for (index, ch) in text.char_indices() {
        match (quote, ch) {
            (Some(open), ch) if ch == open => quote = None,
            (Some(_), _) => {}
            (None, '\'' | '"') => quote = Some(ch),
            (None, '(') => depth += 1,
            (None, ')') => depth = depth.saturating_sub(1),
            (None, ',') if depth == 0 => {
                parts.push(&text[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    parts.push(&text[start..]);
    for part in parts {
        let part = part.trim();
        if let Some(inner) = part
            .strip_prefix('\'')
            .and_then(|rest| rest.strip_suffix('\''))
        {
            values.push(inner.replace("''", "'"));
        } else if part.starts_with('"') {
            values.push(part.to_string());
        } else {
            values.push(part.to_ascii_lowercase());
        }
    }
    values.join(", ")
}

enum RoleSettingChange {
    Set(String, String),
    Reset(Option<String>),
}

fn parse(sql: &str) -> Option<(String, RoleSettingChange)> {
    let trimmed = trim_sql_statement(sql);
    let rest = strip_prefix_ci(trimmed, "ALTER ROLE ")
        .or_else(|| strip_prefix_ci(trimmed, "ALTER USER "))?
        .trim_start();
    let (name, rest) = parse_leading_sql_identifier(rest).ok()?;
    let mut rest = rest.trim_start();
    if let Some(after) = strip_prefix_ci(rest, "IN DATABASE ") {
        // A database-specific default; this server has one database per store.
        let (_, after) = parse_leading_sql_identifier(after).ok()?;
        rest = after.trim_start();
    }
    if let Some(after) = strip_prefix_ci(rest, "SET ") {
        let after = after.trim_start();
        let end = after.find(|c: char| c.is_whitespace() || c == '=')?;
        let parameter = after[..end].to_ascii_lowercase();
        let value = after[end..].trim_start();
        let value = value
            .strip_prefix('=')
            .or_else(|| strip_prefix_ci(value, "TO "))?
            .trim();
        if value.eq_ignore_ascii_case("DEFAULT") {
            return Some((name, RoleSettingChange::Reset(Some(parameter))));
        }
        return Some((
            name,
            RoleSettingChange::Set(parameter, setting_value(value)),
        ));
    }
    if let Some(after) = strip_prefix_ci(rest, "RESET ") {
        let parameter = after.trim().to_ascii_lowercase();
        return Some((
            name,
            RoleSettingChange::Reset((parameter != "all").then_some(parameter)),
        ));
    }
    None
}

impl SqlSession<'_> {
    pub(crate) fn execute_raw_alter_role_settings(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let Some((name, change)) = parse(sql) else {
            return Ok(None);
        };
        let role_name = normalize_role_name(&name);
        // A role may set its own defaults; other roles need role management.
        if role_name != current_user_from_gucs(&self.session_gucs) {
            self.require_role_management_privilege("alter roles")?;
        }
        let mut role = load_role_schema(self.db_ref(), &role_name)?.ok_or_else(|| {
            SqlError::UndefinedRole {
                name: role_name.clone(),
            }
        })?;
        if let RoleSettingChange::Set(parameter, _) = &change {
            if is_protected_security_setting(parameter) || parameter == "bicdb.sql_profile" {
                return Err(SqlError::Unsupported(format!(
                    "parameter \"{parameter}\" cannot be set as a role default"
                )));
            }
        }
        self.capture_role_undo(&role_name)?;
        match change {
            RoleSettingChange::Set(parameter, value) => {
                role.settings.insert(parameter, value);
            }
            RoleSettingChange::Reset(Some(parameter)) => {
                role.settings.remove(&parameter);
            }
            RoleSettingChange::Reset(None) => role.settings.clear(),
        }
        delete_role_record(self.db_mut()?, &role_name)?;
        create_role_record(self.db_mut()?, role, false)?;
        Ok(Some(SqlResult::command("ALTER ROLE")))
    }
}

/// Session defaults of the role a connection logs in as.
pub fn role_session_settings(db: &BicDb, user: &str) -> Result<Vec<(String, String)>> {
    Ok(load_role_schema(db, &normalize_role_name(user))?
        .map(|role| role.settings.into_iter().collect())
        .unwrap_or_default())
}
