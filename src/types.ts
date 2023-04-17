import { DataQuery, DataSourceJsonData } from '@grafana/data';

export interface SnellerQuery extends DataQuery {
  database?: string;
  sql?: string;
}

export const DEFAULT_QUERY: Partial<SnellerQuery> = {
  sql: 'SELECT 1+2',
};

/**
 * These are options configured for each DataSource instance
 */
export interface SnellerDataSourceOptions extends DataSourceJsonData {
  region?: string;
  endpoint?: string;
}

/**
 * Value that is used in the backend, but never sent over HTTP to the frontend
 */
export interface SnellerSecureJsonData {
  token?: string;
}
