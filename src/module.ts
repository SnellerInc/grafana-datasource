import { DataSourcePlugin } from '@grafana/data';
import { DataSource } from './datasource';
import { ConfigEditor } from './components/ConfigEditor';
import { QueryEditor } from './components/QueryEditor';
import { SnellerQuery, SnellerDataSourceOptions } from './types';

export const plugin = new DataSourcePlugin<DataSource, SnellerQuery, SnellerDataSourceOptions>(DataSource)
  .setConfigEditor(ConfigEditor)
  .setQueryEditor(QueryEditor);
