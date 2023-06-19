import { CoreApp, DataSourceInstanceSettings, ScopedVars } from '@grafana/data';
import { DataSourceWithBackend, getTemplateSrv } from '@grafana/runtime';

import { DEFAULT_QUERY, SnellerDataSourceOptions, SnellerQuery } from './types';
import { SnellerVariableSupport } from "./variables";

export class DataSource extends DataSourceWithBackend<SnellerQuery, SnellerDataSourceOptions> {
  constructor(instanceSettings: DataSourceInstanceSettings<SnellerDataSourceOptions>) {
    super(instanceSettings);
    this.variables = new SnellerVariableSupport()
  }

  getDefaultQuery(_: CoreApp): Partial<SnellerQuery> {
    return DEFAULT_QUERY
  }

  applyTemplateVariables(query: SnellerQuery, scopedVars: ScopedVars): Record<string, any> {
    console.log(query.sql)
    return {
      ...query,
      sql: getTemplateSrv().replace(query.sql, scopedVars),
    };
  }
}
