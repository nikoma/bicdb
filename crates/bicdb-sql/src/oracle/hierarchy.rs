//! Depth-first hierarchy expansion with caller-supplied SQL cycle-key equality.
use super::*;

pub(super) struct Node<K> {
    pub row: usize,
    pub key: K,
}

pub(super) struct Visit {
    pub row: usize,
    pub level: usize,
    pub cycle: bool,
}

/// Keys contain evaluated PRIOR expressions, rather than physical row IDs.
/// The SQL caller supplies ordering, expression evaluation, typed key equality
/// and cancellation. Disabling cycle checks implements non-PRIOR expansion.
pub(super) fn traverse<K, Roots, Children, Expand, Equal, Cancel>(
    roots: Roots,
    mut expand: Expand,
    mut equal: Equal,
    mut cancel: Cancel,
    cycle_mode: Option<bool>,
    maximum_nodes: usize,
) -> Result<Vec<Visit>>
where
    Roots: IntoIterator<Item = Result<Node<K>>>,
    Children: IntoIterator<Item = Result<Node<K>>>,
    Expand: FnMut(&Node<K>, usize) -> Result<Children>,
    Equal: FnMut(&K, &K) -> Result<bool>,
    Cancel: FnMut() -> Result<()>,
{
    struct Frame<K> {
        node: Node<K>,
        children: std::vec::IntoIter<Node<K>>,
    }
    let mut output = Vec::new();
    let mut stack: Vec<Frame<K>> = Vec::new();
    let mut pending = 0usize;
    let mut roots = roots.into_iter();
    loop {
        cancel()?;
        let next = match stack.last_mut() {
            Some(frame) => match frame.children.next() {
                Some(node) => {
                    pending -= 1;
                    Some(Ok(node))
                }
                None => {
                    stack.pop();
                    continue;
                }
            },
            None => roots.next(),
        };
        let Some(node) = next else { break };
        let node = node?;
        let level = stack.len() + 1;
        if output.len().saturating_add(pending) >= maximum_nodes {
            return Err(SqlError::data_exception(
                "54000",
                "hierarchy node budget exceeded",
                None,
            ));
        }
        let mut cycle = false;
        let mut children = Vec::new();
        for child in expand(&node, level + 1)? {
            cancel()?;
            let child = child?;
            let mut repeats = false;
            if cycle_mode.is_some() {
                repeats = equal(&node.key, &child.key)?;
                if !repeats {
                    for frame in &stack {
                        if equal(&frame.node.key, &child.key)? {
                            repeats = true;
                            break;
                        }
                    }
                }
            }
            if repeats {
                if cycle_mode == Some(false) {
                    return Err(errors::database_error(
                        -1436,
                        "54001",
                        "CONNECT BY loop in user data",
                    ));
                }
                cycle = true;
                continue;
            }
            if output
                .len()
                .saturating_add(pending)
                .saturating_add(children.len())
                .saturating_add(1)
                >= maximum_nodes
            {
                return Err(SqlError::data_exception(
                    "54000",
                    "hierarchy node budget exceeded",
                    None,
                ));
            }
            children.push(child);
        }
        output.push(Visit {
            row: node.row,
            level,
            cycle,
        });
        pending += children.len();
        stack.push(Frame {
            node,
            children: children.into_iter(),
        });
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sibling_order_and_levels_are_depth_first() {
        let result = traverse(
            vec![Ok(Node { row: 1, key: 1 }), Ok(Node { row: 5, key: 5 })],
            |node, _| {
                Ok(match node.row {
                    1 => vec![2, 3],
                    2 => vec![4],
                    _ => vec![],
                }
                .into_iter()
                .map(|row| Ok(Node { row, key: row }))
                .collect::<Vec<_>>())
            },
            |a, b| Ok(a == b),
            || Ok(()),
            Some(false),
            100,
        )
        .unwrap();
        assert_eq!(
            result.iter().map(|v| (v.row, v.level)).collect::<Vec<_>>(),
            vec![(1, 1), (2, 2), (4, 3), (3, 2), (5, 1)]
        );
    }

    #[test]
    fn different_rows_with_equal_prior_keys_mark_the_parent() {
        let result = traverse(
            vec![Ok(Node { row: 1, key: 1 })],
            |_, _| Ok(vec![Ok(Node { row: 2, key: 1 })]),
            |a, b| Ok(a == b),
            || Ok(()),
            Some(true),
            10,
        )
        .unwrap();
        assert_eq!(result.len(), 1);
        assert!(result[0].cycle);
        assert!(traverse(
            vec![Ok(Node { row: 1, key: 1 })],
            |_, _| Ok(vec![Ok(Node { row: 2, key: 1 })]),
            |a, b| Ok(a == b),
            || Ok(()),
            Some(false),
            10
        )
        .is_err());
    }

    #[test]
    fn non_prior_generators_revisit_rows_and_keep_an_unexpanded_root() {
        for bound in [0, 1, 3] {
            let result = traverse(
                vec![Ok(Node { row: 0, key: () })],
                |_, level| {
                    Ok(if level <= bound {
                        vec![Ok(Node { row: 0, key: () })]
                    } else {
                        vec![]
                    })
                },
                |_, _| Ok(true),
                || Ok(()),
                None,
                10,
            )
            .unwrap();
            assert_eq!(result.len(), bound.max(1));
        }
    }

    #[test]
    fn changing_prior_keys_allow_repeated_physical_rows() {
        let result = traverse(
            vec![Ok(Node { row: 0, key: 1 })],
            |_, level| {
                Ok(if level <= 3 {
                    vec![Ok(Node { row: 0, key: level })]
                } else {
                    vec![]
                })
            },
            |a, b| Ok(a == b),
            || Ok(()),
            Some(false),
            10,
        )
        .unwrap();
        assert_eq!(
            result.iter().map(|v| v.level).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert!(result.iter().all(|v| v.row == 0 && !v.cycle));
    }

    #[test]
    fn unbounded_expansion_obeys_the_budget_and_cancellation() {
        let roots = || vec![Ok(Node { row: 0, key: () })];
        assert!(traverse(
            roots(),
            |_, _| Ok(roots()),
            |_, _| Ok(true),
            || Ok(()),
            None,
            3
        )
        .is_err());
        assert!(traverse(
            roots(),
            |_, _| Ok(roots()),
            |_, _| Ok(true),
            || Err(unsupported("cancelled")),
            None,
            3
        )
        .is_err());
    }
}
