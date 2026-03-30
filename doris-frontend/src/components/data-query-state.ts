import type {
  ExecuteRequest,
  QueryCatalogField,
  QueryCatalogRelationship,
  QueryCatalogTable,
} from '../api/doris';

export interface DataQueryFieldOption {
  value: string;
  label: string;
  table_name: string;
  field_name: string;
  result_label: string;
}

export interface DataQueryFormState {
  action: string;
  table: string;
  relationshipKey?: string;
  selectedColumns: string[];
  selectedColumn?: string;
}

export function buildTableLabel(table: QueryCatalogTable): string {
  if (!table.display_name || table.display_name === table.table_name) {
    return table.table_name;
  }
  return `${table.display_name} (${table.table_name})`;
}

export function buildRelationshipKey(relationship: QueryCatalogRelationship): string {
  return JSON.stringify([
    relationship.related_table_name,
    relationship.source_field_name,
    relationship.target_field_name,
  ]);
}

export function buildRelationshipLabel(relationship: QueryCatalogRelationship): string {
  return (
    relationship.relation_label
    || `${relationship.related_display_name} · ${relationship.relation_description || ''}`.trim()
  );
}

export function encodeFieldSelection(tableName: string, fieldName: string): string {
  return JSON.stringify([tableName, fieldName]);
}

export function buildFieldLabel(tableLabel: string, field: QueryCatalogField): string {
  if (field.display_name && field.display_name !== field.field_name) {
    return `${tableLabel} · ${field.display_name} (${field.field_name})`;
  }
  return `${tableLabel} · ${field.field_name}`;
}

export function buildCompactFieldLabel(field: QueryCatalogField): string {
  return (field.display_name || field.field_name || '').trim();
}

export function buildFieldOptions(
  selectedTable: QueryCatalogTable | undefined,
  selectedRelationship: QueryCatalogRelationship | undefined,
  catalogByTable: Record<string, QueryCatalogTable>,
): DataQueryFieldOption[] {
  if (!selectedTable) {
    return [];
  }

  const tables: QueryCatalogTable[] = [selectedTable];
  if (selectedRelationship) {
    const relatedTable = catalogByTable[selectedRelationship.related_table_name];
    if (relatedTable) {
      tables.push(relatedTable);
    }
  }

  const showTablePrefix = Boolean(selectedRelationship);
  return tables.flatMap((table) =>
    (table.fields || []).map((field) => ({
      value: encodeFieldSelection(table.table_name, field.field_name),
      label: showTablePrefix
        ? buildFieldLabel(table.display_name || table.table_name, field)
        : buildCompactFieldLabel(field),
      table_name: table.table_name,
      field_name: field.field_name,
      result_label: `${table.display_name || table.table_name} ${field.display_name || field.field_name}`,
    })),
  );
}

export function buildDataQueryPayload(args: {
  form: DataQueryFormState;
  params: Record<string, unknown>;
  fieldOptions: DataQueryFieldOption[];
  relationship?: QueryCatalogRelationship;
}): ExecuteRequest {
  const { form, relationship } = args;
  const params = { ...args.params } as Record<string, unknown>;
  const fieldOptionsByValue = Object.fromEntries(args.fieldOptions.map((option) => [option.value, option]));

  if (form.action === 'query') {
    const selectedFields = (form.selectedColumns || [])
      .map((value) => fieldOptionsByValue[value])
      .filter((option): option is DataQueryFieldOption => Boolean(option))
      .map((option) => ({
        table_name: option.table_name,
        field_name: option.field_name,
        label: option.result_label,
      }));

    if (selectedFields.length > 0) {
      params.selected_fields = selectedFields;
    }

    if (relationship) {
      params.join_table = relationship.related_table_name;
      params.join_left_column = relationship.source_field_name;
      params.join_right_column = relationship.target_field_name;
    }

    return {
      action: form.action,
      table: form.table,
      params,
    };
  }

  const selectedColumn = form.selectedColumn ? fieldOptionsByValue[form.selectedColumn] : undefined;
  return {
    action: form.action,
    table: form.table,
    column: selectedColumn?.field_name,
    params,
  };
}
