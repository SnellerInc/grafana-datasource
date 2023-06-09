import { monacoTypes } from '@grafana/ui';
import { SQLMonarchLanguage } from '@grafana/experimental';

export const conf: monacoTypes.languages.LanguageConfiguration = {
    comments: {
        lineComment: '--',
        blockComment: ['/*', '*/'],
    },
    brackets: [
        ['{', '}'],
        ['[', ']'],
        ['(', ')'],
    ],
    autoClosingPairs: [
        { open: '{', close: '}' },
        { open: '[', close: ']' },
        { open: '(', close: ')' },
        { open: '"', close: '"' },
        { open: "'", close: "'" },
    ],
    surroundingPairs: [
        { open: '{', close: '}' },
        { open: '[', close: ']' },
        { open: '(', close: ')' },
        { open: '"', close: '"' },
        { open: "'", close: "'" },
    ],
};

// based on https://github.com/microsoft/monaco-editor/blob/main/src/basic-languages/sql/sql.ts
export const language: SQLMonarchLanguage = {
    defaultToken: '',
    tokenPostfix: '.sql',
    ignoreCase: true,

    brackets: [
        { open: '[', close: ']', token: 'delimiter.square' },
        { open: '(', close: ')', token: 'delimiter.parenthesis' },
    ],

    keywords: [
        'ABORT',
        'ABSOLUTE',
        'ACTION',
        'ADA',
        'ADD',
        'AFTER',
        'ALL',
        'ALLOCATE',
        'ALTER',
        'ALWAYS',
        'ANALYZE',
        'AND',
        'ANY',
        'ARE',
        'AS',
        'ASC',
        'ASSERTION',
        'AT',
        'ATTACH',
        'AUTHORIZATION',
        'AUTOINCREMENT',
        'AVG',
        'BACKUP',
        'BEFORE',
        'BEGIN',
        'BETWEEN',
        'BIT',
        'BIT_LENGTH',
        'BOTH',
        'BREAK',
        'BROWSE',
        'BULK',
        'BY',
        'CASCADE',
        'CASCADED',
        'CASE',
        'CAST',
        'CATALOG',
        'CHAR',
        'CHARACTER',
        'CHARACTER_LENGTH',
        'CHAR_LENGTH',
        'CHECK',
        'CHECKPOINT',
        'CLOSE',
        'CLUSTERED',
        'COALESCE',
        'COLLATE',
        'COLLATION',
        'COLUMN',
        'COMMIT',
        'COMPUTE',
        'CONFLICT',
        'CONNECT',
        'CONNECTION',
        'CONSTRAINT',
        'CONSTRAINTS',
        'CONTAINS',
        'CONTAINSTABLE',
        'CONTINUE',
        'CONVERT',
        'CORRESPONDING',
        'COUNT',
        'CREATE',
        'CROSS',
        'CURRENT',
        'CURRENT_DATE',
        'CURRENT_TIME',
        'CURRENT_TIMESTAMP',
        'CURRENT_USER',
        'CURSOR',
        'DATABASE',
        'DATE',
        'DAY',
        'DBCC',
        'DEALLOCATE',
        'DEC',
        'DECIMAL',
        'DECLARE',
        'DEFAULT',
        'DEFERRABLE',
        'DEFERRED',
        'DELETE',
        'DENY',
        'DESC',
        'DESCRIBE',
        'DESCRIPTOR',
        'DETACH',
        'DIAGNOSTICS',
        'DISCONNECT',
        'DISK',
        'DISTINCT',
        'DISTRIBUTED',
        'DO',
        'DOMAIN',
        'DOUBLE',
        'DROP',
        'DUMP',
        'EACH',
        'ELSE',
        'END',
        'END-EXEC',
        'ERRLVL',
        'ESCAPE',
        'EXCEPT',
        'EXCEPTION',
        'EXCLUDE',
        'EXCLUSIVE',
        'EXEC',
        'EXECUTE',
        'EXISTS',
        'EXIT',
        'EXPLAIN',
        'EXTERNAL',
        'EXTRACT',
        'FAIL',
        'FALSE',
        'FETCH',
        'FILE',
        'FILLFACTOR',
        'FILTER',
        'FIRST',
        'FLOAT',
        'FOLLOWING',
        'FOR',
        'FOREIGN',
        'FORTRAN',
        'FOUND',
        'FREETEXT',
        'FREETEXTTABLE',
        'FROM',
        'FULL',
        'FUNCTION',
        'GENERATED',
        'GET',
        'GLOB',
        'GLOBAL',
        'GO',
        'GOTO',
        'GRANT',
        'GROUP',
        'GROUPS',
        'HAVING',
        'HOLDLOCK',
        'HOUR',
        'IDENTITY',
        'IDENTITYCOL',
        'IDENTITY_INSERT',
        'IF',
        'IGNORE',
        'ILIKE',
        'IMMEDIATE',
        'IN',
        'INCLUDE',
        'INDEX',
        'INDEXED',
        'INDICATOR',
        'INITIALLY',
        'INNER',
        'INPUT',
        'INSENSITIVE',
        'INSERT',
        'INSTEAD',
        'INT',
        'INTEGER',
        'INTERSECT',
        'INTERVAL',
        'INTO',
        'IS',
        'ISNULL',
        'ISOLATION',
        'JOIN',
        'KEY',
        'KILL',
        'LANGUAGE',
        'LAST',
        'LEADING',
        'LEFT',
        'LEVEL',
        'LIKE',
        'LIMIT',
        'LINENO',
        'LOAD',
        'LOCAL',
        'LOWER',
        'MATCH',
        'MATERIALIZED',
        'MAX',
        'MERGE',
        'MIN',
        'MINUTE',
        'MODULE',
        'MONTH',
        'NAMES',
        'NATIONAL',
        'NATURAL',
        'NCHAR',
        'NEXT',
        'NO',
        'NOCHECK',
        'NONCLUSTERED',
        'NONE',
        'NOT',
        'NOTHING',
        'NOTNULL',
        'NULL',
        'NULLIF',
        'NULLS',
        'NUMERIC',
        'OCTET_LENGTH',
        'OF',
        'OFF',
        'OFFSET',
        'OFFSETS',
        'ON',
        'ONLY',
        'OPEN',
        'OPENDATASOURCE',
        'OPENQUERY',
        'OPENROWSET',
        'OPENXML',
        'OPTION',
        'OR',
        'ORDER',
        'OTHERS',
        'OUTER',
        'OUTPUT',
        'OVER',
        'OVERLAPS',
        'PAD',
        'PARTIAL',
        'PARTITION',
        'PASCAL',
        'PERCENT',
        'PIVOT',
        'PLAN',
        'POSITION',
        'PRAGMA',
        'PRECEDING',
        'PRECISION',
        'PREPARE',
        'PRESERVE',
        'PRIMARY',
        'PRINT',
        'PRIOR',
        'PRIVILEGES',
        'PROC',
        'PROCEDURE',
        'PUBLIC',
        'QUERY',
        'RAISE',
        'RAISERROR',
        'RANGE',
        'READ',
        'READTEXT',
        'REAL',
        'RECONFIGURE',
        'RECURSIVE',
        'REFERENCES',
        'REGEXP',
        'REINDEX',
        'RELATIVE',
        'RELEASE',
        'RENAME',
        'REPLACE',
        'REPLICATION',
        'RESTORE',
        'RESTRICT',
        'RETURN',
        'RETURNING',
        'REVERT',
        'REVOKE',
        'RIGHT',
        'ROLLBACK',
        'ROW',
        'ROWCOUNT',
        'ROWGUIDCOL',
        'ROWS',
        'RULE',
        'SAVE',
        'SAVEPOINT',
        'SCHEMA',
        'SCROLL',
        'SECOND',
        'SECTION',
        'SECURITYAUDIT',
        'SELECT',
        'SEMANTICKEYPHRASETABLE',
        'SEMANTICSIMILARITYDETAILSTABLE',
        'SEMANTICSIMILARITYTABLE',
        'SESSION',
        'SESSION_USER',
        'SET',
        'SETUSER',
        'SHUTDOWN',
        'SIMILAR TO',
        'SIZE',
        'SMALLINT',
        'SOME',
        'SPACE',
        'SQL',
        'SQLCA',
        'SQLCODE',
        'SQLERROR',
        'SQLSTATE',
        'SQLWARNING',
        'STATISTICS',
        'SUBSTRING',
        'SUM',
        'SYSTEM_USER',
        'TABLE',
        'TABLESAMPLE',
        'TEMP',
        'TEMPORARY',
        'TEXTSIZE',
        'THEN',
        'TIES',
        'TIME',
        'TIMESTAMP',
        'TIMEZONE_HOUR',
        'TIMEZONE_MINUTE',
        'TO',
        'TOP',
        'TRAILING',
        'TRAN',
        'TRANSACTION',
        'TRANSLATE',
        'TRANSLATION',
        'TRIGGER',
        'TRIM',
        'TRUE',
        'TRUNCATE',
        'TRY_CONVERT',
        'TSEQUAL',
        'UNBOUNDED',
        'UNION',
        'UNIQUE',
        'UNKNOWN',
        'UNPIVOT',
        'UPDATE',
        'UPDATETEXT',
        'UPPER',
        'USAGE',
        'USE',
        'USER',
        'USING',
        'VACUUM',
        'VALUE',
        'VALUES',
        'VARCHAR',
        'VARYING',
        'VIEW',
        'VIRTUAL',
        'WAITFOR',
        'WHEN',
        'WHENEVER',
        'WHERE',
        'WHILE',
        'WINDOW',
        'WITH',
        'WITHIN GROUP',
        'WITHOUT',
        'WORK',
        'WRITE',
        'WRITETEXT',
        'YEAR',
        'ZONE',
    ],
    operators: [
        // Set
        'EXCEPT',
        'INTERSECT',
        'UNION',
        // Join
        'CROSS',
        'INNER',
        'JOIN',
        // Predicates
        'CONTAINS',
        'IS',
        'NULL',
        // Pivoting
        'PIVOT',
        'UNPIVOT',
        // Merging
        'MATCHED',
    ],
    logicalOperators: ['ALL', 'AND', 'ANY', 'BETWEEN', 'EXISTS', 'IN', 'LIKE', 'NOT', 'OR', 'SOME'],
    comparisonOperators: ['<>', '>', '<', '>=', '<=', '=', '!=', '&', '~', '^', '%'],

    builtinFunctions: [
        // Aggregations
        'COUNT',
        'MIN',
        'MAX',
        'EARLIEST',
        'LATEST',
        'SUM',
        'AVG',
        'VARIANCE',
        'VARIANCE_POP',
        'STDDEV',
        'STDDEV_POP',
        'BIT_AND',
        'BIT_OR',
        'BIT_XOR',
        'BOOL_AND',
        'EVERY',
        'BOOL_OR',
        'APPROX_COUNT_DISTINCT',
        'ROW_NUMBER',
        'RANK',
        'DENSE_RANK',
        'SNELLER_DATASHAPE',
        // Bit Manipulation
        'BIT_COUNT',
        // Math Functions
        'ABS',
        'CBRT',
        'EXP',
        'EXPM1',
        'EXP2',
        'EXP10',
        'HYPOT',
        'LN',
        'LN1P',
        'LOG',
        'LOG2',
        'LOG10',
        'POW',
        'POWER',
        'SIGN',
        'SQRT',
        // Trigonometric Functions
        'DEGREES',
        'RADIANS',
        'SIN',
        'COS',
        'TAN',
        'ASIN',
        'ACOS',
        'ATAN',
        'ATAN2',
        // Rounding Functions
        'ROUND',
        'ROUND_EVEN',
        'TRUNC',
        'FLOOR',
        'CEIL',
        'CEILING',
        // GEO Functions
        'GEO_DISTANCE',
        'GEO_HASH',
        'GEO_TILE_X',
        'GEO_TILE_Y',
        'GEO_TILE_ES',
        // Built-in Functions
        'DATE_ADD',
        'DATE_BIN',
        'DATE_DIFF',
        'DATE_TRUNC',
        'EXTRACT',
        'UTCNOW',
        'LEAST',
        'GREATEST',
        'WIDTH_BUCKET',
        'TIME_BUCKET',
        'TO_UNIX_EPOCH',
        'TO_UNIX_MICRO',
        'TRIM',
        'LTRIM',
        'RTRIM',
        'SIZE',
        'ARRAY_SIZE',
        'ARRAY_CONTAINS',
        'ARRAY_POSITION',
        'OCTET_LENGTH',
        'CHAR_LENGTH',
        'CHARACTER_LENGTH',
        'LOWER',
        'UPPER',
        'EQUALS_CI',
        'SUBSTRING',
        'SLIT_PART',
        'IS_SUBNET_OF',
        'EQUALS_FUZZY',
        'EQUALS_FUZZY_UNICODE',
        'CONTAINS_FUZZY',
        'CONTAINS_FUZZY_UNICODE',
        'CAST',
        'TYPE_BIT',
        'TABLE_GLOB',
        'TABLE_PATTERN'
    ],
    builtinVariables: [
        // empty
    ],
    pseudoColumns: [
        // empty
    ],
    tokenizer: {
        root: [
            { include: '@templateVariables' },
            { include: '@macros' },
            { include: '@comments' },
            { include: '@whitespace' },
            { include: '@pseudoColumns' },
            { include: '@numbers' },
            { include: '@strings' },
            { include: '@complexIdentifiers' },
            { include: '@scopes' },
            { include: '@schemaTable' },
            [/[;,.]/, 'delimiter'],
            [/[()]/, '@brackets'],
            [
                /[\w@#$|<|>|=|!|%|&|+|\|-|*|/|~|^]+/,
                {
                    cases: {
                        '@operators': 'operator',
                        '@comparisonOperators': 'operator',
                        '@logicalOperators': 'operator',
                        '@builtinVariables': 'predefined',
                        '@builtinFunctions': 'predefined',
                        '@keywords': 'keyword',
                        '@default': 'identifier',
                    },
                },
            ],
        ],
        templateVariables: [[/\$[a-zA-Z0-9]+/, 'variable']],
        macros: [[/\$__[a-zA-Z0-9-_]+/, 'type']],
        schemaTable: [
            [/(\w+)\./, 'identifier'],
            [/(\w+\.\w+)/, 'identifier'],
        ],
        whitespace: [[/\s+/, 'white']],
        comments: [
            [/--+.*/, 'comment'],
            [/\/\*/, { token: 'comment.quote', next: '@comment' }],
        ],
        comment: [
            [/[^*/]+/, 'comment'],
            // Not supporting nested comments, as nested comments seem to not be standard?
            // i.e. http://stackoverflow.com/questions/728172/are-there-multiline-comment-delimiters-in-sql-that-are-vendor-agnostic
            // [/\/\*/, { token: 'comment.quote', next: '@push' }],    // nested comment not allowed :-(
            [/\*\//, { token: 'comment.quote', next: '@pop' }],
            [/./, 'comment'],
        ],
        pseudoColumns: [
            [
                /[$][A-Za-z_][\w@#$]*/,
                {
                    cases: {
                        '@pseudoColumns': 'predefined',
                        '@default': 'identifier',
                    },
                },
            ],
        ],
        numbers: [
            [/0[xX][0-9a-fA-F]*/, 'number'],
            [/[$][+-]*\d*(\.\d*)?/, 'number'],
            [/((\d+(\.\d*)?)|(\.\d+))([eE][\-+]?\d+)?/, 'number'],
        ],
        strings: [
            [/N'/, { token: 'string', next: '@string' }],
            [/'/, { token: 'string', next: '@string' }],
        ],
        string: [
            [/[^']+/, 'string'],
            [/''/, 'string'],
            [/'/, { token: 'string', next: '@pop' }],
        ],
        complexIdentifiers: [
            [/\[/, { token: 'identifier.quote', next: '@bracketedIdentifier' }],
            [/"/, { token: 'identifier.quote', next: '@quotedIdentifier' }],
        ],
        bracketedIdentifier: [
            [/[^\]]+/, 'identifier'],
            [/]]/, 'identifier'],
            [/]/, { token: 'identifier.quote', next: '@pop' }],
        ],
        quotedIdentifier: [
            [/[^"]+/, 'identifier'],
            [/""/, 'identifier'],
            [/"/, { token: 'identifier.quote', next: '@pop' }],
        ],
        scopes: [
            [/BEGIN\s+(DISTRIBUTED\s+)?TRAN(SACTION)?\b/i, 'keyword'],
            [/BEGIN\s+TRY\b/i, { token: 'keyword.try' }],
            [/END\s+TRY\b/i, { token: 'keyword.try' }],
            [/BEGIN\s+CATCH\b/i, { token: 'keyword.catch' }],
            [/END\s+CATCH\b/i, { token: 'keyword.catch' }],
            [/(BEGIN|CASE)\b/i, { token: 'keyword.block' }],
            [/END\b/i, { token: 'keyword.block' }],
            [/WHEN\b/i, { token: 'keyword.choice' }],
            [/THEN\b/i, { token: 'keyword.choice' }],
        ],
    },
};
