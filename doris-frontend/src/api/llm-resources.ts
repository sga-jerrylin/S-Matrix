export interface NormalizedLLMResource {
  name: string;
  provider: string;
  model: string;
  endpoint: string;
  temperature?: number;
  maxTokens?: number;
  apiKeyConfigured: boolean;
  lastTestStatus?: string;
  lastTestError?: string;
  raw: Record<string, any>;
}

const normalizeOptionalNumber = (
  value: unknown,
  validator: (parsed: number) => boolean,
): number | undefined => {
  if (value === undefined || value === null || value === '') {
    return undefined;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || !validator(parsed)) {
    return undefined;
  }
  return parsed;
};

export const normalizeLLMResource = (resource: Record<string, any>): NormalizedLLMResource => {
  const properties = (resource?.properties || {}) as Record<string, any>;
  const apiKeyValue = properties['ai.api_key'] ?? properties.api_key ?? '';
  const temperatureValue =
    resource?.temperature ?? properties['ai.temperature'] ?? properties.temperature ?? undefined;
  const maxTokensValue =
    resource?.max_tokens ?? resource?.maxTokens ?? properties['ai.max_tokens'] ?? properties.max_tokens ?? undefined;

  return {
    name: String(resource?.name ?? resource?.ResourceName ?? resource?.Name ?? '').trim(),
    provider: String(
      resource?.provider ??
        properties['ai.provider_type'] ??
        properties.provider_type ??
        resource?.ResourceType ??
        '',
    ).trim(),
    model: String(resource?.model ?? properties['ai.model_name'] ?? properties.model_name ?? '').trim(),
    endpoint: String(resource?.endpoint ?? properties['ai.endpoint'] ?? properties.endpoint ?? '').trim(),
    temperature: normalizeOptionalNumber(temperatureValue, (parsed) => parsed >= 0 && parsed <= 1),
    maxTokens: normalizeOptionalNumber(maxTokensValue, (parsed) => parsed > 0),
    apiKeyConfigured: Boolean(
      resource?.api_key_configured ??
        (typeof apiKeyValue === 'string' ? apiKeyValue.trim() : apiKeyValue),
    ),
    lastTestStatus: String(resource?.last_test_status ?? resource?.lastTestStatus ?? '').trim() || undefined,
    lastTestError: String(resource?.last_test_error ?? resource?.lastTestError ?? '').trim() || undefined,
    raw: resource,
  };
};
