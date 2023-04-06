import React from 'react';
import { InlineField, AsyncSelect, CodeEditor, ActionMeta } from '@grafana/ui';
import { QueryEditorProps, SelectableValue } from '@grafana/data';
import { DataSource } from '../datasource';
import { MyDataSourceOptions, MyQuery } from '../types';

type Props = QueryEditorProps<DataSource, MyQuery, MyDataSourceOptions>;

export function QueryEditor({ datasource, query, onChange, onRunQuery }: Props) {

  const onDatabaseChange = (value: SelectableValue<string>, actionMeta: ActionMeta) => {
    onChange({ ...query, database: value?.value });
  };

  const onQueryTextChange = (queryText: string) => {
    onChange({ ...query, queryText });
  };

  const { database, queryText } = query;

  const loadDatabaseOptions = async () => {
    try {
      const response = await datasource.getDatabases();
      return (response.data.map<SelectableValue<string>>((x) => ({
        label: x.name,
        value: x.name,
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
          cacheOptions={true}
          defaultOptions={true}
          onChange={onDatabaseChange}
          value={{ label: database, value: database}}
          isClearable={true}
          isSearchable={false}
        />
      </InlineField>
      <CodeEditor
        height="200px"
        showLineNumbers={true}
        language="sql"
        onBlur={onQueryTextChange}
        value={queryText || 'SELECT * FROM `table` LIMIT 1'}
      />
    </div>
  );
}
