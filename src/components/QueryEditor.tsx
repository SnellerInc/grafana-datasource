import React, { useState } from 'react';
import { InlineField, AsyncSelect, ActionMeta } from '@grafana/ui';
import { QueryEditorProps, SelectableValue } from '@grafana/data';
import { DataSource } from '../datasource';
import { SnellerDataSourceOptions, SnellerQuery } from '../types';
import { SQLEditor } from '@grafana/experimental';
import { SnellerLanguage } from "../sneller_sql";

type Props = QueryEditorProps<DataSource, SnellerQuery, SnellerDataSourceOptions>;

export function QueryEditor({ datasource, query, onChange, onRunQuery }: Props) {
    const [database, setDatabase] = useState<string>(query.database || "");
    const [table, setTable] = useState<string>("");

    const onDatabaseChange = (value: SelectableValue<string>, actionMeta: ActionMeta) => {
        onChange({ ...query, database: value?.value });
        setDatabase(value?.value || "")
        setTable("")
    };

    const onTableChange = (value: SelectableValue<string>, actionMeta: ActionMeta) => {
        setTable(value?.value || "")
    };

    const onSqlChange = (sql: string) => {
        onChange({ ...query, sql });
    };

    const { sql } = query;

    const loadDatabaseOptions = async () => {
        try {
            const response = await datasource.getResource('databases') as string[];
            return (response.map((x) => ({
                label: x,
                value: x,
            })));
        } catch {
            return []
        }
    };

    const loadTableOptions = async () => {
        if (!database) {
            return []
        }
        try {
            const response = await datasource.getResource('tables/' + database) as string[];
            return (response.map((x) => ({
                label: x,
                value: x,
            })));
        } catch {
            return []
        }
    };

    return (
        <div className="gf-form-group">
            <InlineField label="Database" labelWidth={24} tooltip="The database name" grow>
                <AsyncSelect
                    loadOptions={loadDatabaseOptions}
                    cacheOptions
                    defaultOptions
                    onChange={onDatabaseChange}
                    value={{ label: database, value: database }}
                    isClearable
                    isSearchable={false}
                />
            </InlineField>
            <InlineField label="Table" labelWidth={24} tooltip="The table name" grow>
                <AsyncSelect
                    key={database}
                    loadOptions={loadTableOptions}
                    cacheOptions
                    defaultOptions
                    onChange={onTableChange}
                    value={{ label: table, value: table }}
                    isClearable
                    isSearchable={false}
                />
            </InlineField>
            <SQLEditor
                height={200}
                onChange={onSqlChange}
                query={sql!}
                language={new SnellerLanguage()}
            />
        </div>
    );
}
