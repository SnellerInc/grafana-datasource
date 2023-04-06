import { DataQuery, DataSourceJsonData } from '@grafana/data';

export interface MyQuery extends DataQuery {
  database?: string;
  queryText: string;
}

export const DEFAULT_QUERY: Partial<MyQuery> = {
  queryText: 'SELECT 1+2',
};

/**
 * These are options configured for each DataSource instance
 */
export interface MyDataSourceOptions extends DataSourceJsonData {
  region?: string;
  endpoint?: string;
}

/**
 * Value that is used in the backend, but never sent over HTTP to the frontend
 */
export interface MySecureJsonData {
  token?: string;
}

export interface SnellerDatabase {
  name: string;
}
