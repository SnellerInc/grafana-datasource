import React, { useRef } from 'react';
import {InlineField, AsyncSelect, ActionMeta, monacoTypes, Monaco} from '@grafana/ui';
import { QueryEditorProps, SelectableValue } from '@grafana/data';
import { DataSource } from '../datasource';
import { SnellerDataSourceOptions, SnellerQuery } from '../types';
import { getStandardSQLCompletionProvider, LanguageDefinition, SQLEditor, SQLMonarchLanguage } from '@grafana/experimental';
import { language, conf } from "../sneller_sql";
import { TableIdentifier } from "@grafana/experimental/dist/sql-editor/types";

type Props = QueryEditorProps<DataSource, SnellerQuery, SnellerDataSourceOptions>;

export function QueryEditor({ datasource, query, onChange, onRunQuery }: Props) {

    const databaseRef = useRef(query.database)

    const onDatabaseChange = (value: SelectableValue<string>, actionMeta: ActionMeta) => {
        onChange({ ...query, database: value?.value });
        databaseRef.current = value?.value
    };

    const onQueryChange = (q: string, processQuery: boolean) => {
        onChange({ ...query, database: databaseRef.current, sql: q });
    };

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

    const loadTableOptions = async (database?: string) => {
        if (!database) {
            return []
        }
        try {
            const response = await datasource.getResource('tables/' + encodeURIComponent(database)) as string[];
            return (response.map((x) => ({
                name: x,
            })));
        } catch {
            return []
        }
    };

    const loadColumnOptions = async (table: TableIdentifier) => {
        if (!databaseRef || !table || !table.table) {
            return []
        }
        try {
            const response = await datasource.getResource(
                'columns/' + encodeURIComponent(databaseRef.current!) + '/' + encodeURIComponent(table.table)
            ) as string[];
            return (response.map((x) => ({
                name: x,
            })));
        } catch {
            return []
        }
    };

    const snellerLanguageDefinition: LanguageDefinition = {
        id: 'sneller_sql',
        loader: () => new Promise<{
            language: SQLMonarchLanguage;
            conf: monacoTypes.languages.LanguageConfiguration;
        }>((resolve) => resolve({ language: language, conf: conf })),
        completionProvider: (m: Monaco, language?: SQLMonarchLanguage) => {
            let provider = getStandardSQLCompletionProvider(m, language!)
            provider.tables = {
                resolve: async () => {
                    return loadTableOptions(databaseRef.current)
                }
            };
            provider.columns = {
                resolve: loadColumnOptions
            };
            return provider;
        }
    }

    return (
        <div className="gf-form-group">
            <InlineField label="Database" labelWidth={24} tooltip="The database name" grow>
                <AsyncSelect
                    loadOptions={loadDatabaseOptions}
                    cacheOptions
                    defaultOptions
                    onChange={onDatabaseChange}
                    value={ databaseRef ? { label: databaseRef.current!, value: databaseRef.current! } : undefined }
                    isClearable
                    isSearchable={true}
                    allowCustomValue={true}
                />
            </InlineField>
            <SQLEditor
                height={200}
                onChange={onQueryChange}
                query={query.sql!}
                language={snellerLanguageDefinition}
            />
        </div>
    );
}
