import {
  DataQueryRequest,
  DataQueryResponse,
  DataSourceApi,
  DataSourceInstanceSettings,
} from '@grafana/data';

import { getBackendSrv, getTemplateSrv, isFetchError } from '@grafana/runtime';
import { MyQuery, MyDataSourceOptions, DEFAULT_QUERY, SnellerDatabase } from './types';

import _ from 'lodash';
import defaults from 'lodash/defaults';
import { lastValueFrom } from 'rxjs';

export class DataSource extends DataSourceApi<MyQuery, MyDataSourceOptions> {
  baseUrl: string

  constructor(instanceSettings: DataSourceInstanceSettings<MyDataSourceOptions>) {
    super(instanceSettings);

    this.baseUrl = instanceSettings.url!
  }

  async query(options: DataQueryRequest<MyQuery>): Promise<DataQueryResponse> {
    const defaultErrorMessage = 'Failed to execute query';

    const promises = options.targets.map(async (target) => {
      const query = defaults(target, DEFAULT_QUERY);

      try {
        const sql = getTemplateSrv().replace(query.queryText, options.scopedVars)
        const response = await this.executeQuery('/executeQuery', sql, query.database);
      
        /**
         * The endpoint returns:
         *
         * [
         *   {
         *     "key": "value"
         *   }
         * ]
         */
        if (!response.data) {
          throw new Error('Remote endpoint did not return any data.');
        }

        return response.data;
      } catch(err) {
        let message = '';
        if (_.isString(err)) {
          message = err;
        } else if (isFetchError(err)) {
          message = 'Fetch error: ' + (err.statusText ? err.statusText : defaultErrorMessage);
          if (err.data && err.data.message) {
            message += ': ' + err.data.message
          }
        }
        throw new Error(message)
      }
    });

    return Promise.all(promises).then((data) => ({ data }));
  }

  async testDatasource() {
    const defaultErrorMessage = 'Failed to contact "snellerd" service';

    try {
      await this.executeQuery('/executeQuery', 'SELECT 1+2');
      return {
        status: 'success',
        message: 'Success',
      };
    } catch (err) {
      let message = '';
      if (_.isString(err)) {
        message = err;
      } else if (isFetchError(err)) {
        message = 'Fetch error: ' + (err.statusText ? err.statusText : defaultErrorMessage);
        if (err.data && err.data.message) {
          message += ': ' + err.data.message
        }
      }
      return {
        status: 'error',
        message,
      };
    }
  }

  async getDatabases() {
    const response = getBackendSrv().fetch<SnellerDatabase[]>({
      url: `${this.baseUrl}/snellerd/databases`,
      method: 'GET',
    });
    return lastValueFrom(response);
  }
  
  async executeQuery(url: string, query: string, database?: string) {
    const response = getBackendSrv().fetch<any[]>({
      url: `${this.baseUrl}/snellerd${url}${database?.length ? `?database=${database}` : ''}`,
      method: 'POST',
      headers: { 
        'Accept': 'application/json',
      },
      data: query,
    });
    return lastValueFrom(response);
  }
}
