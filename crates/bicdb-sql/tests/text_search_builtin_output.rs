//! Pins the built-in text search configurations' output on a fixed corpus.
//!
//! Stored tsvector columns, generated columns and expression indexes hold
//! this output, so any change to it is an upgrade event: an entry here that
//! changes must come with a release note telling users to rebuild them.
//! Values match PostgreSQL 18.4 except where BicDB's tokenizer splits signed
//! numbers and digit-hyphen-digit tokens differently (a known difference).
use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

/// (statement, expected output with columns joined by `|`).
const CORPUS: &[(&str, &str)] = &[
    (
        r#"SELECT to_tsvector('english', 'The quick brown foxes jumped over the lazy dogs')"#,
        r#"'brown':3 'dog':9 'fox':4 'jump':5 'lazi':8 'quick':2"#,
    ),
    (
        r#"SELECT to_tsvector('english', 'Running runners ran; they are running again!')"#,
        r#"'ran':3 'run':1,6 'runner':2"#,
    ),
    (
        r#"SELECT to_tsvector('english', 'PostgreSQL 18.4 was released on 2026-05-11 with 1,000 fixes')"#,
        r#"'000':12 '05':8 '1':11 '11':9 '18.4':2 '2026':7 '2026-05-11':6 'fix':13 'postgresql':1 'releas':4"#,
    ),
    (
        r#"SELECT to_tsvector('english', 'Visit https://www.example.com/docs/files/index.html or http://api.example.org/v2/users')"#,
        r#"'/docs/files/index.html':4 '/v2/users':8 'api.example.org':7 'api.example.org/v2/users':6 'visit':1 'www.example.com':3 'www.example.com/docs/files/index.html':2"#,
    ),
    (
        r#"SELECT to_tsvector('english', 'Mail john.doe@example.com and ops-team@example.org')"#,
        r#"'john.doe@example.com':2 'mail':1 'ops-team@example.org':4"#,
    ),
    (
        r#"SELECT to_tsvector('english', 'covid19s cases rose; abc123s and x86-64 builds, foo-bar2 state-of-the-art')"#,
        r#"'64':8 'abc123s':4 'art':17 'bar2':12 'build':9 'case':2 'covid19s':1 'foo':11 'foo-bar2':10 'rose':3 'state':14 'state-of-the-art':13 'x86':7 'x86-64':6"#,
    ),
    (
        r#"SELECT to_tsvector('english', 'Paths /usr/local/bin and /var/logs/services are hosts like api.services.example.net')"#,
        r#"'/usr/local/bin':2 '/var/logs/services':4 'api.services.example.net':8 'host':6 'like':7 'path':1"#,
    ),
    (
        r#"SELECT to_tsvector('english', 'Version 1.2.3 float 3.14 sci 1e5 int -45 uint 42')"#,
        r#"'1.2.3':2 '1e5':6 '3.14':4 '42':10 '45':8 'float':3 'int':7 'sci':5 'uint':9 'version':1"#,
    ),
    (
        r#"SELECT to_tsvector('english', 'Наши собаки бегают быстро running dogs')"#,
        r#"'dog':6 'run':5 'бегают':3 'быстро':4 'наши':1 'собаки':2"#,
    ),
    (
        r#"SELECT to_tsvector('english', 'Die Häuser waren größer als erwartet')"#,
        r#"'al':5 'die':1 'erwartet':6 'größer':4 'häuser':2 'waren':3"#,
    ),
    (
        r#"SELECT to_tsvector('english', 'naïve café résumé coöperate')"#,
        r#"'café':2 'coöper':4 'naïv':1 'résumé':3"#,
    ),
    (
        r#"SELECT to_tsvector('english', 'it''s John''s book; don''t stop')"#,
        r#"'book':5 'john':3 'stop':8"#,
    ),
    (r#"SELECT to_tsvector('english', '')"#, r#""#),
    (
        r#"SELECT to_tsvector('english', 'a an the of and or')"#,
        r#""#,
    ),
    (
        r#"SELECT to_tsquery('english', 'running & dogs')"#,
        r#"'run' & 'dog'"#,
    ),
    (
        r#"SELECT to_tsquery('english', 'foxes:* | jumped')"#,
        r#"'fox':* | 'jump'"#,
    ),
    (
        r#"SELECT to_tsquery('english', '!lazy & (dog <-> fox)')"#,
        r#"!'lazi' & 'dog' <-> 'fox'"#,
    ),
    (r#"SELECT to_tsquery('english', 'files')"#, r#"'file'"#),
    (
        r#"SELECT to_tsquery('english', 'covid19s')"#,
        r#"'covid19s'"#,
    ),
    (
        r#"SELECT plainto_tsquery('english', 'the running dogs'), phraseto_tsquery('english', 'the running dogs'), websearch_to_tsquery('english', 'the running dogs')"#,
        r#"'run' & 'dog'|'run' <-> 'dog'|'run' & 'dog'"#,
    ),
    (
        r#"SELECT plainto_tsquery('english', 'covid19s cases'), phraseto_tsquery('english', 'covid19s cases'), websearch_to_tsquery('english', 'covid19s cases')"#,
        r#"'covid19s' & 'case'|'covid19s' <-> 'case'|'covid19s' & 'case'"#,
    ),
    (
        r#"SELECT plainto_tsquery('english', '/docs/files'), phraseto_tsquery('english', '/docs/files'), websearch_to_tsquery('english', '/docs/files')"#,
        r#"'/docs/files'|'/docs/files'|'/docs/files'"#,
    ),
    (
        r#"SELECT plainto_tsquery('english', 'api.services.example.net'), phraseto_tsquery('english', 'api.services.example.net'), websearch_to_tsquery('english', 'api.services.example.net')"#,
        r#"'api.services.example.net'|'api.services.example.net'|'api.services.example.net'"#,
    ),
    (
        r#"SELECT plainto_tsquery('english', 'running or walking'), phraseto_tsquery('english', 'running or walking'), websearch_to_tsquery('english', 'running or walking')"#,
        r#"'run' & 'walk'|'run' <2> 'walk'|'run' | 'walk'"#,
    ),
    (
        r#"SELECT to_tsvector('simple', 'The quick brown foxes jumped over the lazy dogs')"#,
        r#"'brown':3 'dogs':9 'foxes':4 'jumped':5 'lazy':8 'over':6 'quick':2 'the':1,7"#,
    ),
    (
        r#"SELECT to_tsvector('simple', 'Running runners ran; they are running again!')"#,
        r#"'again':7 'are':5 'ran':3 'runners':2 'running':1,6 'they':4"#,
    ),
    (
        r#"SELECT to_tsvector('simple', 'PostgreSQL 18.4 was released on 2026-05-11 with 1,000 fixes')"#,
        r#"'000':12 '05':8 '1':11 '11':9 '18.4':2 '2026':7 '2026-05-11':6 'fixes':13 'on':5 'postgresql':1 'released':4 'was':3 'with':10"#,
    ),
    (
        r#"SELECT to_tsvector('simple', 'Visit https://www.example.com/docs/files/index.html or http://api.example.org/v2/users')"#,
        r#"'/docs/files/index.html':4 '/v2/users':8 'api.example.org':7 'api.example.org/v2/users':6 'or':5 'visit':1 'www.example.com':3 'www.example.com/docs/files/index.html':2"#,
    ),
    (
        r#"SELECT to_tsvector('simple', 'Mail john.doe@example.com and ops-team@example.org')"#,
        r#"'and':3 'john.doe@example.com':2 'mail':1 'ops-team@example.org':4"#,
    ),
    (
        r#"SELECT to_tsvector('simple', 'covid19s cases rose; abc123s and x86-64 builds, foo-bar2 state-of-the-art')"#,
        r#"'64':8 'abc123s':4 'and':5 'art':17 'bar2':12 'builds':9 'cases':2 'covid19s':1 'foo':11 'foo-bar2':10 'of':15 'rose':3 'state':14 'state-of-the-art':13 'the':16 'x86':7 'x86-64':6"#,
    ),
    (
        r#"SELECT to_tsvector('simple', 'Paths /usr/local/bin and /var/logs/services are hosts like api.services.example.net')"#,
        r#"'/usr/local/bin':2 '/var/logs/services':4 'and':3 'api.services.example.net':8 'are':5 'hosts':6 'like':7 'paths':1"#,
    ),
    (
        r#"SELECT to_tsvector('simple', 'Version 1.2.3 float 3.14 sci 1e5 int -45 uint 42')"#,
        r#"'1.2.3':2 '1e5':6 '3.14':4 '42':10 '45':8 'float':3 'int':7 'sci':5 'uint':9 'version':1"#,
    ),
    (
        r#"SELECT to_tsvector('simple', 'Наши собаки бегают быстро running dogs')"#,
        r#"'dogs':6 'running':5 'бегают':3 'быстро':4 'наши':1 'собаки':2"#,
    ),
    (
        r#"SELECT to_tsvector('simple', 'Die Häuser waren größer als erwartet')"#,
        r#"'als':5 'die':1 'erwartet':6 'größer':4 'häuser':2 'waren':3"#,
    ),
    (
        r#"SELECT to_tsvector('simple', 'naïve café résumé coöperate')"#,
        r#"'café':2 'coöperate':4 'naïve':1 'résumé':3"#,
    ),
    (
        r#"SELECT to_tsvector('simple', 'it''s John''s book; don''t stop')"#,
        r#"'book':5 'don':6 'it':1 'john':3 's':2,4 'stop':8 't':7"#,
    ),
    (r#"SELECT to_tsvector('simple', '')"#, r#""#),
    (
        r#"SELECT to_tsvector('simple', 'a an the of and or')"#,
        r#"'a':1 'an':2 'and':5 'of':4 'or':6 'the':3"#,
    ),
    (
        r#"SELECT to_tsquery('simple', 'running & dogs')"#,
        r#"'running' & 'dogs'"#,
    ),
    (
        r#"SELECT to_tsquery('simple', 'foxes:* | jumped')"#,
        r#"'foxes':* | 'jumped'"#,
    ),
    (
        r#"SELECT to_tsquery('simple', '!lazy & (dog <-> fox)')"#,
        r#"!'lazy' & 'dog' <-> 'fox'"#,
    ),
    (r#"SELECT to_tsquery('simple', 'files')"#, r#"'files'"#),
    (
        r#"SELECT to_tsquery('simple', 'covid19s')"#,
        r#"'covid19s'"#,
    ),
    (
        r#"SELECT plainto_tsquery('simple', 'the running dogs'), phraseto_tsquery('simple', 'the running dogs'), websearch_to_tsquery('simple', 'the running dogs')"#,
        r#"'the' & 'running' & 'dogs'|'the' <-> 'running' <-> 'dogs'|'the' & 'running' & 'dogs'"#,
    ),
    (
        r#"SELECT plainto_tsquery('simple', 'covid19s cases'), phraseto_tsquery('simple', 'covid19s cases'), websearch_to_tsquery('simple', 'covid19s cases')"#,
        r#"'covid19s' & 'cases'|'covid19s' <-> 'cases'|'covid19s' & 'cases'"#,
    ),
    (
        r#"SELECT plainto_tsquery('simple', '/docs/files'), phraseto_tsquery('simple', '/docs/files'), websearch_to_tsquery('simple', '/docs/files')"#,
        r#"'/docs/files'|'/docs/files'|'/docs/files'"#,
    ),
    (
        r#"SELECT plainto_tsquery('simple', 'api.services.example.net'), phraseto_tsquery('simple', 'api.services.example.net'), websearch_to_tsquery('simple', 'api.services.example.net')"#,
        r#"'api.services.example.net'|'api.services.example.net'|'api.services.example.net'"#,
    ),
    (
        r#"SELECT plainto_tsquery('simple', 'running or walking'), phraseto_tsquery('simple', 'running or walking'), websearch_to_tsquery('simple', 'running or walking')"#,
        r#"'running' & 'or' & 'walking'|'running' <-> 'or' <-> 'walking'|'running' | 'walking'"#,
    ),
    (
        r#"SELECT to_tsvector('russian', 'The quick brown foxes jumped over the lazy dogs')"#,
        r#"'brown':3 'dog':9 'fox':4 'jump':5 'lazi':8 'quick':2"#,
    ),
    (
        r#"SELECT to_tsvector('russian', 'Running runners ran; they are running again!')"#,
        r#"'ran':3 'run':1,6 'runner':2"#,
    ),
    (
        r#"SELECT to_tsvector('russian', 'PostgreSQL 18.4 was released on 2026-05-11 with 1,000 fixes')"#,
        r#"'000':12 '05':8 '1':11 '11':9 '18.4':2 '2026':7 '2026-05-11':6 'fix':13 'postgresql':1 'releas':4"#,
    ),
    (
        r#"SELECT to_tsvector('russian', 'Visit https://www.example.com/docs/files/index.html or http://api.example.org/v2/users')"#,
        r#"'/docs/files/index.html':4 '/v2/users':8 'api.example.org':7 'api.example.org/v2/users':6 'visit':1 'www.example.com':3 'www.example.com/docs/files/index.html':2"#,
    ),
    (
        r#"SELECT to_tsvector('russian', 'Mail john.doe@example.com and ops-team@example.org')"#,
        r#"'john.doe@example.com':2 'mail':1 'ops-team@example.org':4"#,
    ),
    (
        r#"SELECT to_tsvector('russian', 'covid19s cases rose; abc123s and x86-64 builds, foo-bar2 state-of-the-art')"#,
        r#"'64':8 'abc123s':4 'art':17 'bar2':12 'build':9 'case':2 'covid19s':1 'foo':11 'foo-bar2':10 'rose':3 'state':14 'state-of-the-art':13 'x86':7 'x86-64':6"#,
    ),
    (
        r#"SELECT to_tsvector('russian', 'Paths /usr/local/bin and /var/logs/services are hosts like api.services.example.net')"#,
        r#"'/usr/local/bin':2 '/var/logs/services':4 'api.services.example.net':8 'host':6 'like':7 'path':1"#,
    ),
    (
        r#"SELECT to_tsvector('russian', 'Version 1.2.3 float 3.14 sci 1e5 int -45 uint 42')"#,
        r#"'1.2.3':2 '1e5':6 '3.14':4 '42':10 '45':8 'float':3 'int':7 'sci':5 'uint':9 'version':1"#,
    ),
    (
        r#"SELECT to_tsvector('russian', 'Наши собаки бегают быстро running dogs')"#,
        r#"'dog':6 'run':5 'бега':3 'быстр':4 'наш':1 'собак':2"#,
    ),
    (
        r#"SELECT to_tsvector('russian', 'Die Häuser waren größer als erwartet')"#,
        r#"'al':5 'die':1 'erwartet':6 'größer':4 'häuser':2 'waren':3"#,
    ),
    (
        r#"SELECT to_tsvector('russian', 'naïve café résumé coöperate')"#,
        r#"'café':2 'coöperate':4 'naïve':1 'résumé':3"#,
    ),
    (
        r#"SELECT to_tsvector('russian', 'it''s John''s book; don''t stop')"#,
        r#"'book':5 'john':3 'stop':8"#,
    ),
    (r#"SELECT to_tsvector('russian', '')"#, r#""#),
    (
        r#"SELECT to_tsvector('russian', 'a an the of and or')"#,
        r#""#,
    ),
    (
        r#"SELECT to_tsquery('russian', 'running & dogs')"#,
        r#"'run' & 'dog'"#,
    ),
    (
        r#"SELECT to_tsquery('russian', 'foxes:* | jumped')"#,
        r#"'fox':* | 'jump'"#,
    ),
    (
        r#"SELECT to_tsquery('russian', '!lazy & (dog <-> fox)')"#,
        r#"!'lazi' & 'dog' <-> 'fox'"#,
    ),
    (r#"SELECT to_tsquery('russian', 'files')"#, r#"'file'"#),
    (
        r#"SELECT to_tsquery('russian', 'covid19s')"#,
        r#"'covid19s'"#,
    ),
    (
        r#"SELECT plainto_tsquery('russian', 'the running dogs'), phraseto_tsquery('russian', 'the running dogs'), websearch_to_tsquery('russian', 'the running dogs')"#,
        r#"'run' & 'dog'|'run' <-> 'dog'|'run' & 'dog'"#,
    ),
    (
        r#"SELECT plainto_tsquery('russian', 'covid19s cases'), phraseto_tsquery('russian', 'covid19s cases'), websearch_to_tsquery('russian', 'covid19s cases')"#,
        r#"'covid19s' & 'case'|'covid19s' <-> 'case'|'covid19s' & 'case'"#,
    ),
    (
        r#"SELECT plainto_tsquery('russian', '/docs/files'), phraseto_tsquery('russian', '/docs/files'), websearch_to_tsquery('russian', '/docs/files')"#,
        r#"'/docs/files'|'/docs/files'|'/docs/files'"#,
    ),
    (
        r#"SELECT plainto_tsquery('russian', 'api.services.example.net'), phraseto_tsquery('russian', 'api.services.example.net'), websearch_to_tsquery('russian', 'api.services.example.net')"#,
        r#"'api.services.example.net'|'api.services.example.net'|'api.services.example.net'"#,
    ),
    (
        r#"SELECT plainto_tsquery('russian', 'running or walking'), phraseto_tsquery('russian', 'running or walking'), websearch_to_tsquery('russian', 'running or walking')"#,
        r#"'run' & 'walk'|'run' <2> 'walk'|'run' | 'walk'"#,
    ),
    (
        r#"SELECT to_tsvector('german', 'The quick brown foxes jumped over the lazy dogs')"#,
        r#"'brown':3 'dog':9 'fox':4 'jumped':5 'lazy':8 'over':6 'quick':2 'the':1,7"#,
    ),
    (
        r#"SELECT to_tsvector('german', 'Running runners ran; they are running again!')"#,
        r#"'again':7 'are':5 'ran':3 'runn':2 'running':1,6 'they':4"#,
    ),
    (
        r#"SELECT to_tsvector('german', 'PostgreSQL 18.4 was released on 2026-05-11 with 1,000 fixes')"#,
        r#"'000':12 '05':8 '1':11 '11':9 '18.4':2 '2026':7 '2026-05-11':6 'fix':13 'on':5 'postgresql':1 'released':4 'with':10"#,
    ),
    (
        r#"SELECT to_tsvector('german', 'Visit https://www.example.com/docs/files/index.html or http://api.example.org/v2/users')"#,
        r#"'/docs/files/index.html':4 '/v2/users':8 'api.example.org':7 'api.example.org/v2/users':6 'or':5 'visit':1 'www.example.com':3 'www.example.com/docs/files/index.html':2"#,
    ),
    (
        r#"SELECT to_tsvector('german', 'Mail john.doe@example.com and ops-team@example.org')"#,
        r#"'and':3 'john.doe@example.com':2 'mail':1 'ops-team@example.org':4"#,
    ),
    (
        r#"SELECT to_tsvector('german', 'covid19s cases rose; abc123s and x86-64 builds, foo-bar2 state-of-the-art')"#,
        r#"'64':8 'abc123s':4 'and':5 'art':17 'bar2':12 'build':9 'cas':2 'covid19s':1 'foo':11 'foo-bar2':10 'of':15 'ros':3 'stat':14 'state-of-the-art':13 'the':16 'x86':7 'x86-64':6"#,
    ),
    (
        r#"SELECT to_tsvector('german', 'Paths /usr/local/bin and /var/logs/services are hosts like api.services.example.net')"#,
        r#"'/usr/local/bin':2 '/var/logs/services':4 'and':3 'api.services.example.net':8 'are':5 'host':6 'lik':7 'path':1"#,
    ),
    (
        r#"SELECT to_tsvector('german', 'Version 1.2.3 float 3.14 sci 1e5 int -45 uint 42')"#,
        r#"'1.2.3':2 '1e5':6 '3.14':4 '42':10 '45':8 'float':3 'int':7 'sci':5 'uint':9 'version':1"#,
    ),
    (
        r#"SELECT to_tsvector('german', 'Наши собаки бегают быстро running dogs')"#,
        r#"'dog':6 'running':5 'бегают':3 'быстро':4 'наши':1 'собаки':2"#,
    ),
    (
        r#"SELECT to_tsvector('german', 'Die Häuser waren größer als erwartet')"#,
        r#"'erwart':6 'gross':4 'haus':2"#,
    ),
    (
        r#"SELECT to_tsvector('german', 'naïve café résumé coöperate')"#,
        r#"'café':2 'cooperat':4 'naïv':1 'résumé':3"#,
    ),
    (
        r#"SELECT to_tsvector('german', 'it''s John''s book; don''t stop')"#,
        r#"'book':5 'don':6 'it':1 'john':3 's':2,4 'stop':8 't':7"#,
    ),
    (r#"SELECT to_tsvector('german', '')"#, r#""#),
    (
        r#"SELECT to_tsvector('german', 'a an the of and or')"#,
        r#"'a':1 'and':5 'of':4 'or':6 'the':3"#,
    ),
    (
        r#"SELECT to_tsquery('german', 'running & dogs')"#,
        r#"'running' & 'dog'"#,
    ),
    (
        r#"SELECT to_tsquery('german', 'foxes:* | jumped')"#,
        r#"'fox':* | 'jumped'"#,
    ),
    (
        r#"SELECT to_tsquery('german', '!lazy & (dog <-> fox)')"#,
        r#"!'lazy' & 'dog' <-> 'fox'"#,
    ),
    (r#"SELECT to_tsquery('german', 'files')"#, r#"'fil'"#),
    (
        r#"SELECT to_tsquery('german', 'covid19s')"#,
        r#"'covid19s'"#,
    ),
    (
        r#"SELECT plainto_tsquery('german', 'the running dogs'), phraseto_tsquery('german', 'the running dogs'), websearch_to_tsquery('german', 'the running dogs')"#,
        r#"'the' & 'running' & 'dog'|'the' <-> 'running' <-> 'dog'|'the' & 'running' & 'dog'"#,
    ),
    (
        r#"SELECT plainto_tsquery('german', 'covid19s cases'), phraseto_tsquery('german', 'covid19s cases'), websearch_to_tsquery('german', 'covid19s cases')"#,
        r#"'covid19s' & 'cas'|'covid19s' <-> 'cas'|'covid19s' & 'cas'"#,
    ),
    (
        r#"SELECT plainto_tsquery('german', '/docs/files'), phraseto_tsquery('german', '/docs/files'), websearch_to_tsquery('german', '/docs/files')"#,
        r#"'/docs/files'|'/docs/files'|'/docs/files'"#,
    ),
    (
        r#"SELECT plainto_tsquery('german', 'api.services.example.net'), phraseto_tsquery('german', 'api.services.example.net'), websearch_to_tsquery('german', 'api.services.example.net')"#,
        r#"'api.services.example.net'|'api.services.example.net'|'api.services.example.net'"#,
    ),
    (
        r#"SELECT plainto_tsquery('german', 'running or walking'), phraseto_tsquery('german', 'running or walking'), websearch_to_tsquery('german', 'running or walking')"#,
        r#"'running' & 'or' & 'walking'|'running' <-> 'or' <-> 'walking'|'running' | 'walking'"#,
    ),
    (
        r#"SELECT to_tsvector('The quick brown foxes jumped over the lazy dogs')"#,
        r#"'brown':3 'dog':9 'fox':4 'jump':5 'lazi':8 'quick':2"#,
    ),
    (
        r#"SELECT to_tsvector('Running runners ran; they are running again!')"#,
        r#"'ran':3 'run':1,6 'runner':2"#,
    ),
    (
        r#"SELECT to_tsvector('PostgreSQL 18.4 was released on 2026-05-11 with 1,000 fixes')"#,
        r#"'000':12 '05':8 '1':11 '11':9 '18.4':2 '2026':7 '2026-05-11':6 'fix':13 'postgresql':1 'releas':4"#,
    ),
    (
        r#"SELECT to_tsvector('Visit https://www.example.com/docs/files/index.html or http://api.example.org/v2/users')"#,
        r#"'/docs/files/index.html':4 '/v2/users':8 'api.example.org':7 'api.example.org/v2/users':6 'visit':1 'www.example.com':3 'www.example.com/docs/files/index.html':2"#,
    ),
    (
        r#"SELECT to_tsvector('Mail john.doe@example.com and ops-team@example.org')"#,
        r#"'john.doe@example.com':2 'mail':1 'ops-team@example.org':4"#,
    ),
    (
        r#"SELECT to_tsvector('covid19s cases rose; abc123s and x86-64 builds, foo-bar2 state-of-the-art')"#,
        r#"'64':8 'abc123s':4 'art':17 'bar2':12 'build':9 'case':2 'covid19s':1 'foo':11 'foo-bar2':10 'rose':3 'state':14 'state-of-the-art':13 'x86':7 'x86-64':6"#,
    ),
    (
        r#"SELECT to_tsvector('Paths /usr/local/bin and /var/logs/services are hosts like api.services.example.net')"#,
        r#"'/usr/local/bin':2 '/var/logs/services':4 'api.services.example.net':8 'host':6 'like':7 'path':1"#,
    ),
    (
        r#"SELECT to_tsvector('Version 1.2.3 float 3.14 sci 1e5 int -45 uint 42')"#,
        r#"'1.2.3':2 '1e5':6 '3.14':4 '42':10 '45':8 'float':3 'int':7 'sci':5 'uint':9 'version':1"#,
    ),
    (
        r#"SELECT to_tsvector('Наши собаки бегают быстро running dogs')"#,
        r#"'dog':6 'run':5 'бегают':3 'быстро':4 'наши':1 'собаки':2"#,
    ),
    (
        r#"SELECT to_tsvector('Die Häuser waren größer als erwartet')"#,
        r#"'al':5 'die':1 'erwartet':6 'größer':4 'häuser':2 'waren':3"#,
    ),
    (
        r#"SELECT to_tsvector('naïve café résumé coöperate')"#,
        r#"'café':2 'coöper':4 'naïv':1 'résumé':3"#,
    ),
    (
        r#"SELECT to_tsvector('it''s John''s book; don''t stop')"#,
        r#"'book':5 'john':3 'stop':8"#,
    ),
    (r#"SELECT to_tsvector('')"#, r#""#),
    (r#"SELECT to_tsvector('a an the of and or')"#, r#""#),
    (
        r#"SELECT plainto_tsquery('the running dogs'), websearch_to_tsquery('the running dogs')"#,
        r#"'run' & 'dog'|'run' & 'dog'"#,
    ),
    (
        r#"SELECT plainto_tsquery('covid19s cases'), websearch_to_tsquery('covid19s cases')"#,
        r#"'covid19s' & 'case'|'covid19s' & 'case'"#,
    ),
    (
        r#"SELECT plainto_tsquery('/docs/files'), websearch_to_tsquery('/docs/files')"#,
        r#"'/docs/files'|'/docs/files'"#,
    ),
    (
        r#"SELECT plainto_tsquery('api.services.example.net'), websearch_to_tsquery('api.services.example.net')"#,
        r#"'api.services.example.net'|'api.services.example.net'"#,
    ),
    (
        r#"SELECT plainto_tsquery('running or walking'), websearch_to_tsquery('running or walking')"#,
        r#"'run' & 'walk'|'run' | 'walk'"#,
    ),
];

#[test]
fn built_in_configurations_produce_pinned_output() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let mut mismatches = Vec::new();
    for (sql, expected) in CORPUS {
        let result = session.execute(sql).unwrap();
        let actual = result.rows[0]
            .iter()
            .map(|value| value.to_cell())
            .collect::<Vec<_>>()
            .join("|");
        if actual != *expected {
            mismatches.push(format!("{sql}\n  expected {expected}\n  actual   {actual}"));
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}
