//! Shared SQL LIKE wildcard matching for expression and persisted index evaluation.

/// Whether the final escape character has no character to escape.
pub fn like_pattern_ends_with_escape(pattern: &str, escape: Option<char>) -> bool {
    escape.is_some_and(|escape| {
        pattern
            .chars()
            .rev()
            .take_while(|character| *character == escape)
            .count()
            % 2
            == 1
    })
}

/// Match a validated LIKE pattern against a sequence of SQL characters.
pub fn like_match_chars(value: &[char], pattern: &[char], escape: Option<char>) -> bool {
    let (mut vi, mut pi) = (0usize, 0usize);
    let mut star = None::<usize>;
    let mut star_value = 0usize;
    while vi < value.len() {
        if pi < pattern.len() {
            if Some(pattern[pi]) == escape && pi + 1 < pattern.len() {
                pi += 1;
                if pattern[pi] == value[vi] {
                    vi += 1;
                    pi += 1;
                    continue;
                }
            } else if pattern[pi] == '%' {
                star = Some(pi);
                pi += 1;
                star_value = vi;
                continue;
            } else if pattern[pi] == '_' || pattern[pi] == value[vi] {
                vi += 1;
                pi += 1;
                continue;
            }
        }
        if let Some(star_idx) = star {
            pi = star_idx + 1;
            star_value += 1;
            vi = star_value;
        } else {
            return false;
        }
    }
    while pi < pattern.len() && pattern[pi] == '%' && escape != Some('%') {
        pi += 1;
    }
    pi == pattern.len()
}
