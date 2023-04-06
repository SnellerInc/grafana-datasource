import React, { ChangeEvent } from 'react';
import { InlineField, Input, SecretInput, Select, ActionMeta } from '@grafana/ui';
import { DataSourcePluginOptionsEditorProps, SelectableValue } from '@grafana/data';
import { MyDataSourceOptions, MySecureJsonData } from '../types';

interface Props extends DataSourcePluginOptionsEditorProps<MyDataSourceOptions> {}

export function ConfigEditor(props: Props) {
  const { onOptionsChange, options } = props;

  interface SnellerRegion {
      label: string;
      value: string;
  }

  const snellerRegions: SnellerRegion[] = [
    {
      label: 'US East (N. Virginia)',
      value: 'us-east-1',
    },
    {
      label: 'US East (Ohio)',
      value: 'us-east-2',
    },
    {
      label: 'US West (N. California)',
      value: 'us-west-1',
    },
    {
      label: 'Europe (Ireland)',
      value: 'eu-west-1'
    },
    {
      label: 'Custom Endpoint',
      value: 'custom',
    }
  ];

  const onRegionChange = (value: SelectableValue<string>, actionMeta: ActionMeta) => {
    let endpoint = (value.value === 'custom') 
      ? options.jsonData.endpoint 
      : `https://snellerd-master.${value.value}.sneller-dev.io`

    const jsonData = {
      ...options.jsonData,
      region: value.value,
      endpoint: endpoint,
    };
    onOptionsChange({ ...options, jsonData });
  };

  const onEndpointChange = (event: ChangeEvent<HTMLInputElement>) => {
    const jsonData = {
      ...options.jsonData,
      endpoint: event.target.value,
    };
    onOptionsChange({ ...options, jsonData });
  };

  // Secure field (only sent to the backend)
  const onTokenChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      secureJsonData: {
        token: event.target.value,
      },
    });
  };

  const onResetToken = () => {
    onOptionsChange({
      ...options,
      secureJsonFields: {
        ...options.secureJsonFields,
        token: false,
      },
      secureJsonData: {
        ...options.secureJsonData,
        token: '',
      },
    });
  };

  const { jsonData, secureJsonFields } = options;
  const secureJsonData = (options.secureJsonData || {}) as MySecureJsonData;

  return (
    <div className="gf-form-group">
      <InlineField label="Sneller Region" labelWidth={24} tooltip='' grow>
        <Select 
          options={snellerRegions} 
          onChange={onRegionChange} 
          value={jsonData.region} 
        />
      </InlineField>
      <InlineField label="Sneller Endpoint" labelWidth={24} tooltip='' disabled={jsonData.region !== 'custom'} required grow>
        <Input
          onChange={onEndpointChange}
          value={jsonData.endpoint}
          placeholder="The Sneller query endpoint"
          required
        />
      </InlineField>
      <InlineField label="Sneller Token" labelWidth={24} tooltip='' required grow>
        <SecretInput
          isConfigured={(secureJsonFields && secureJsonFields.token) as boolean}
          value={secureJsonData.token}
          placeholder="The Sneller authentication token"
          onReset={onResetToken}
          onChange={onTokenChange}
          required
        />
      </InlineField>
    </div>
  );
}
