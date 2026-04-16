import type { NaturalQueryRequest } from '../api/doris';

export interface NaturalQueryFormState {
  query: string;
  selectedResource?: string;
  apiKey?: string;
  selectedTables?: string[];
}

export function buildNaturalQueryPayload(form: NaturalQueryFormState): NaturalQueryRequest {
  const selectedTables = (form.selectedTables || [])
    .map((tableName) => tableName.trim())
    .filter(Boolean);

  const payload: NaturalQueryRequest = {
    query: form.query.trim(),
    resource_name: form.selectedResource || undefined,
  };

  const apiKey = form.apiKey?.trim();
  if (apiKey) {
    payload.api_key = apiKey;
  };

  if (selectedTables.length > 0) {
    payload.table_names = selectedTables;
  }

  return payload;
}
