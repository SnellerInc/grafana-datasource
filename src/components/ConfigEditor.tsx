import React, { ChangeEvent } from 'react';
import { InlineField, Input, SecretInput, Select, ActionMeta } from '@grafana/ui';
import { DataSourcePluginOptionsEditorProps, SelectableValue } from '@grafana/data';
import { SnellerDataSourceOptions, SnellerSecureJsonData } from '../types';

interface Props extends DataSourcePluginOptionsEditorProps<SnellerDataSourceOptions> {}

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
      label: 'Playground',
      value: 'play'
    },
    {
      label: 'Custom Endpoint',
      value: 'custom',
    }
  ];

  const onRegionChange = (value: SelectableValue<string>, actionMeta: ActionMeta) => {
    let endpoint = ''
    switch (value.value) {
      case 'custom':
        endpoint = ''
        break
      case 'play':
        endpoint = `https://play.sneller.ai`
        break
      default:
        endpoint = `https://snellerd-production.${value.value}.sneller.ai`
        break
    }

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
  const secureJsonData = (options.secureJsonData || {}) as SnellerSecureJsonData;

  return (
      <div className="gf-form-group">
        <InlineField label="Sneller Region" labelWidth={24} tooltip='' required grow>
          <Select
              options={snellerRegions}
              onChange={onRegionChange}
              value={jsonData.region || 'us-east-1'}
          />
        </InlineField>
        <InlineField label="Sneller Endpoint" labelWidth={24} tooltip='' disabled={jsonData.region !== 'custom'} required={jsonData.region === 'custom'} grow>
          <Input
              onChange={onEndpointChange}
              value={jsonData.endpoint}
              placeholder="The Sneller query endpoint"
              required={jsonData.region === 'custom'}
          />
        </InlineField>
        <InlineField label="Sneller Token" labelWidth={24} tooltip='' disabled={jsonData.region === 'play'} required={jsonData.region !== 'play'} grow>
          <SecretInput
              isConfigured={(secureJsonFields && secureJsonFields.token) as boolean}
              value={secureJsonData.token}
              placeholder="The Sneller authentication token"
              onReset={onResetToken}
              onChange={onTokenChange}
              required={jsonData.region !== 'play'}
          />
        </InlineField>
      </div>
  );
}
