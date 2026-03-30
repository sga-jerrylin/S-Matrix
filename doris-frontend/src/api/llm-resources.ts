export interface NormalizedLLMResource {
  name: string;
  provider: string;
  model: string;
  endpoint: string;
  apiKeyConfigured: boolean;
  raw: Record<string, any>;
}

export const normalizeLLMResource = (resource: Record<string, any>): NormalizedLLMResource => {
  const properties = (resource?.properties || {}) as Record<string, any>;
  const apiKeyValue = properties['ai.api_key'] ?? properties.api_key ?? '';

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
    apiKeyConfigured: Boolean(
      resource?.api_key_configured ??
        (typeof apiKeyValue === 'string' ? apiKeyValue.trim() : apiKeyValue),
    ),
    raw: resource,
  };
};
