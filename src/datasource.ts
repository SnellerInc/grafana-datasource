import { DataSourceInstanceSettings, CoreApp } from '@grafana/data';
import { DataSourceWithBackend } from '@grafana/runtime';

import { SnellerQuery, SnellerDataSourceOptions, DEFAULT_QUERY } from './types';
import { SnellerVariableSupport } from "./variables";

export class DataSource extends DataSourceWithBackend<SnellerQuery, SnellerDataSourceOptions> {
  constructor(instanceSettings: DataSourceInstanceSettings<SnellerDataSourceOptions>) {
    super(instanceSettings);
    this.variables = new SnellerVariableSupport()
  }

  getDefaultQuery(_: CoreApp): Partial<SnellerQuery> {
    return DEFAULT_QUERY
  }
}
