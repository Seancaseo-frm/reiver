import { ref, computed, shallowRef } from 'vue';
import { tableFromIPC, tableToIPC } from 'apache-arrow';
import axios from 'axios';

const ARROW_CONTENT_TYPE = 'application/vnd.apache.arrow.stream';

/**
 * Composable for executing warehouse queries with Arrow IPC transport
 * and client-side sorting, filtering, pagination, aggregation, and export.
 *
 * The server returns binary Arrow IPC when our UI sends the Accept header;
 * all post-query manipulation happens locally without additional round-trips.
 */
export function useArrowQuery() {
  const arrowTable = shallowRef(null);
  const columns = ref([]);
  const executing = ref(false);
  const error = ref(null);
  const executionTime = ref(0);
  const totalRows = ref(0);

  const sortColumn = ref(null);
  const sortDirection = ref('asc');
  const filters = ref({});
  const currentPage = ref(0);
  const pageSize = ref(100);

  /**
   * Execute a query against the warehouse and receive an Arrow Table.
   */
  async function executeQuery(projectId, sql, limit) {
    if (!sql.trim() || executing.value) return;
    executing.value = true;
    error.value = null;
    arrowTable.value = null;
    columns.value = [];
    totalRows.value = 0;
    sortColumn.value = null;
    sortDirection.value = 'asc';
    filters.value = {};
    currentPage.value = 0;

    const startTime = Date.now();
    try {
      const response = await axios.post(
        `/api/projects/${projectId}/warehouse/query?format=arrow`,
        { sql, limit },
        {
          responseType: 'arraybuffer',
          headers: { Accept: ARROW_CONTENT_TYPE },
        }
      );
      executionTime.value = Date.now() - startTime;

      const table = tableFromIPC(new Uint8Array(response.data));
      arrowTable.value = table;
      totalRows.value = table.numRows;
      columns.value = table.schema.fields.map((f) => ({
        name: f.name,
        data_type: f.type.toString(),
        nullable: f.nullable,
      }));
    } catch (e) {
      executionTime.value = Date.now() - startTime;
      if (e.response && e.response.data instanceof ArrayBuffer) {
        const text = new TextDecoder().decode(e.response.data);
        try {
          const json = JSON.parse(text);
          error.value = json.message || json.error || text;
        } catch {
          error.value = text;
        }
      } else {
        error.value = e.response?.data?.message || e.response?.data?.error || e.message;
      }
    } finally {
      executing.value = false;
    }
  }

  // ----- Working rows: apply filters + sort + pagination -----

  /** Get all row indices that pass current filters. */
  function getFilteredIndices() {
    const table = arrowTable.value;
    if (!table) return [];

    const numRows = table.numRows;
    const activeFilters = Object.entries(filters.value).filter(
      ([, f]) => f.active
    );

    if (activeFilters.length === 0) {
      return Array.from({ length: numRows }, (_, i) => i);
    }

    const indices = [];
    for (let i = 0; i < numRows; i++) {
      let pass = true;
      for (const [colName, filter] of activeFilters) {
        const col = table.getChild(colName);
        if (!col) continue;
        const val = col.get(i);
        if (!applyPredicate(val, filter)) {
          pass = false;
          break;
        }
      }
      if (pass) indices.push(i);
    }
    return indices;
  }

  function applyPredicate(value, filter) {
    switch (filter.op) {
      case 'eq':
        return value === filter.value;
      case 'neq':
        return value !== filter.value;
      case 'gt':
        return value > filter.value;
      case 'gte':
        return value >= filter.value;
      case 'lt':
        return value < filter.value;
      case 'lte':
        return value <= filter.value;
      case 'in':
        return Array.isArray(filter.value) && filter.value.includes(value);
      case 'like': {
        if (typeof value !== 'string') return false;
        const escaped = filter.value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        const pattern = escaped.replace(/%/g, '.*').replace(/_/g, '.');
        return new RegExp(`^${pattern}$`, 'i').test(value);
      }
      case 'contains':
        return typeof value === 'string' && value.includes(filter.value);
      case 'is_null':
        return value === null || value === undefined;
      case 'is_not_null':
        return value !== null && value !== undefined;
      default:
        return true;
    }
  }

  /** Sort indices by the current sort column/direction. */
  function sortIndices(indices) {
    if (!sortColumn.value || !arrowTable.value) return indices;
    const col = arrowTable.value.getChild(sortColumn.value);
    if (!col) return indices;

    const sorted = [...indices];
    const dir = sortDirection.value === 'asc' ? 1 : -1;
    sorted.sort((a, b) => {
      const va = col.get(a);
      const vb = col.get(b);
      if (va === null || va === undefined) return dir;
      if (vb === null || vb === undefined) return -dir;
      if (va < vb) return -dir;
      if (va > vb) return dir;
      return 0;
    });
    return sorted;
  }

  /** The fully processed rows (filtered, sorted, paginated) as plain JS arrays. */
  const processedResult = computed(() => {
    const table = arrowTable.value;
    if (!table || table.numRows === 0) {
      return { rows: [], filteredCount: 0, pageCount: 0 };
    }

    let indices = getFilteredIndices();
    indices = sortIndices(indices);

    const filteredCount = indices.length;
    const pageCount = Math.max(1, Math.ceil(filteredCount / pageSize.value));

    const start = currentPage.value * pageSize.value;
    const end = Math.min(start + pageSize.value, filteredCount);
    const pageIndices = indices.slice(start, end);

    const colCount = table.schema.fields.length;
    const rows = [];
    for (const idx of pageIndices) {
      const row = new Array(colCount);
      for (let c = 0; c < colCount; c++) {
        const col = table.getChildAt(c);
        const v = col ? col.get(idx) : null;
        row[c] = arrowValueToJS(v);
      }
      rows.push(row);
    }

    return { rows, filteredCount, pageCount };
  });

  function arrowValueToJS(v) {
    if (v === null || v === undefined) return null;
    if (typeof v === 'bigint') return Number(v);
    if (v instanceof Date) return v.toISOString();
    if (ArrayBuffer.isView(v)) return Array.from(v);
    return v;
  }

  // ----- Sorting API -----

  function setSort(columnName, direction) {
    sortColumn.value = columnName;
    sortDirection.value = direction || 'asc';
    currentPage.value = 0;
  }

  function toggleSort(columnName) {
    if (sortColumn.value === columnName) {
      sortDirection.value = sortDirection.value === 'asc' ? 'desc' : 'asc';
    } else {
      sortColumn.value = columnName;
      sortDirection.value = 'asc';
    }
    currentPage.value = 0;
  }

  // ----- Filtering API -----

  function setFilter(columnName, op, value) {
    filters.value = {
      ...filters.value,
      [columnName]: { op, value, active: true },
    };
    currentPage.value = 0;
  }

  function removeFilter(columnName) {
    const copy = { ...filters.value };
    delete copy[columnName];
    filters.value = copy;
    currentPage.value = 0;
  }

  function clearFilters() {
    filters.value = {};
    currentPage.value = 0;
  }

  // ----- Pagination API -----

  function setPage(page) {
    currentPage.value = Math.max(0, page);
  }

  function nextPage() {
    const pageCount = processedResult.value.pageCount;
    if (currentPage.value < pageCount - 1) {
      currentPage.value++;
    }
  }

  function prevPage() {
    if (currentPage.value > 0) {
      currentPage.value--;
    }
  }

  function setPageSize(size) {
    pageSize.value = size;
    currentPage.value = 0;
  }

  // ----- Aggregation API -----

  function aggregate(columnName, aggFn) {
    const table = arrowTable.value;
    if (!table) return null;
    const col = table.getChild(columnName);
    if (!col) return null;

    const indices = getFilteredIndices();
    if (indices.length === 0) return null;

    switch (aggFn) {
      case 'count':
        return indices.length;
      case 'count_distinct': {
        const seen = new Set();
        for (const i of indices) {
          const v = col.get(i);
          if (v !== null && v !== undefined) seen.add(v);
        }
        return seen.size;
      }
      case 'sum': {
        let sum = 0;
        for (const i of indices) {
          const v = col.get(i);
          if (v !== null && v !== undefined) sum += Number(v);
        }
        return sum;
      }
      case 'avg': {
        let sum = 0;
        let count = 0;
        for (const i of indices) {
          const v = col.get(i);
          if (v !== null && v !== undefined) {
            sum += Number(v);
            count++;
          }
        }
        return count > 0 ? sum / count : null;
      }
      case 'min': {
        let min = Infinity;
        for (const i of indices) {
          const v = col.get(i);
          if (v !== null && v !== undefined && Number(v) < min) min = Number(v);
        }
        return min === Infinity ? null : min;
      }
      case 'max': {
        let max = -Infinity;
        for (const i of indices) {
          const v = col.get(i);
          if (v !== null && v !== undefined && Number(v) > max) max = Number(v);
        }
        return max === -Infinity ? null : max;
      }
      default:
        return null;
    }
  }

  /** Compute summary stats for all numeric columns in a single pass per column. */
  function columnStats() {
    const table = arrowTable.value;
    if (!table) return {};

    const indices = getFilteredIndices();
    if (indices.length === 0) return {};

    const stats = {};
    for (const field of table.schema.fields) {
      const dtype = field.type.toString().toLowerCase();
      const isNumeric =
        dtype.includes('int') ||
        dtype.includes('float') ||
        dtype.includes('decimal') ||
        dtype.includes('double');
      if (!isNumeric) continue;

      const col = table.getChild(field.name);
      if (!col) continue;

      let sum = 0;
      let min = Infinity;
      let max = -Infinity;
      let count = 0;

      for (const i of indices) {
        const v = col.get(i);
        if (v === null || v === undefined) continue;
        const n = Number(v);
        sum += n;
        if (n < min) min = n;
        if (n > max) max = n;
        count++;
      }

      stats[field.name] = {
        sum: count > 0 ? sum : null,
        avg: count > 0 ? sum / count : null,
        min: count > 0 ? min : null,
        max: count > 0 ? max : null,
        count: indices.length,
      };
    }
    return stats;
  }

  // ----- Export API -----

  function exportCSV() {
    const table = arrowTable.value;
    if (!table) return '';

    const indices = sortIndices(getFilteredIndices());
    const header = table.schema.fields.map((f) => f.name).join(',');
    const rows = indices.map((idx) => {
      return table.schema.fields
        .map((f, colIdx) => {
          const col = table.getChildAt(colIdx);
          const val = col ? arrowValueToJS(col.get(idx)) : null;
          if (val === null || val === undefined) return '';
          const str = String(val);
          if (str.includes(',') || str.includes('"') || str.includes('\n')) {
            return `"${str.replace(/"/g, '""')}"`;
          }
          return str;
        })
        .join(',');
    });
    return [header, ...rows].join('\n');
  }

  function exportJSON() {
    const table = arrowTable.value;
    if (!table) return '[]';

    const indices = sortIndices(getFilteredIndices());
    const result = indices.map((idx) => {
      const obj = {};
      for (const field of table.schema.fields) {
        const col = table.getChild(field.name);
        obj[field.name] = col ? arrowValueToJS(col.get(idx)) : null;
      }
      return obj;
    });
    return JSON.stringify(result, null, 2);
  }

  function downloadFile(content, filename, mimeType) {
    const blob = new Blob([content], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  }

  function downloadCSV(filename = 'query-results.csv') {
    downloadFile(exportCSV(), filename, 'text/csv');
  }

  function downloadJSON(filename = 'query-results.json') {
    downloadFile(exportJSON(), filename, 'application/json');
  }

  function downloadArrowIPC(filename = 'query-results.arrow') {
    const table = arrowTable.value;
    if (!table) return;
    const ipcBytes = tableToIPC(table, 'stream');
    const blob = new Blob([ipcBytes], { type: ARROW_CONTENT_TYPE });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  }

  // ----- Client-side caching (IndexedDB) -----

  const DB_NAME = 'reiver_query_cache';
  const STORE_NAME = 'arrow_results';
  const CACHE_TTL_MS = 5 * 60 * 1000;

  function openCacheDB() {
    return new Promise((resolve, reject) => {
      const request = indexedDB.open(DB_NAME, 1);
      request.onupgradeneeded = () => {
        const db = request.result;
        if (!db.objectStoreNames.contains(STORE_NAME)) {
          db.createObjectStore(STORE_NAME);
        }
      };
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
  }

  async function hashSQL(sql) {
    const data = new TextEncoder().encode(sql);
    const hashBuffer = await crypto.subtle.digest('SHA-256', data);
    return Array.from(new Uint8Array(hashBuffer))
      .map((b) => b.toString(16).padStart(2, '0'))
      .join('');
  }

  async function getCached(projectId, sql) {
    try {
      const db = await openCacheDB();
      const key = `${projectId}:${await hashSQL(sql)}`;
      return new Promise((resolve) => {
        const tx = db.transaction(STORE_NAME, 'readonly');
        const store = tx.objectStore(STORE_NAME);
        const request = store.get(key);
        request.onsuccess = () => {
          const entry = request.result;
          if (!entry || Date.now() - entry.timestamp > CACHE_TTL_MS) {
            resolve(null);
          } else {
            resolve(entry.data);
          }
        };
        request.onerror = () => resolve(null);
      });
    } catch {
      return null;
    }
  }

  async function setCached(projectId, sql, arrayBuffer) {
    try {
      const db = await openCacheDB();
      const key = `${projectId}:${await hashSQL(sql)}`;
      const tx = db.transaction(STORE_NAME, 'readwrite');
      const store = tx.objectStore(STORE_NAME);
      store.put({ data: arrayBuffer, timestamp: Date.now() }, key);
    } catch {
      // Cache write failures are non-critical
    }
  }

  /**
   * Execute a query with client-side caching.
   * Checks IndexedDB first, falls back to server if cache miss.
   */
  async function executeQueryCached(projectId, sql, limit) {
    if (!sql.trim() || executing.value) return;

    const cached = await getCached(projectId, sql);
    if (cached) {
      executing.value = true;
      try {
        const table = tableFromIPC(new Uint8Array(cached));
        arrowTable.value = table;
        totalRows.value = table.numRows;
        columns.value = table.schema.fields.map((f) => ({
          name: f.name,
          data_type: f.type.toString(),
          nullable: f.nullable,
        }));
        executionTime.value = 0;
        error.value = null;
        sortColumn.value = null;
        sortDirection.value = 'asc';
        filters.value = {};
        currentPage.value = 0;
      } catch {
        await executeQueryAndCache(projectId, sql, limit);
      } finally {
        executing.value = false;
      }
      return;
    }

    await executeQueryAndCache(projectId, sql, limit);
  }

  async function executeQueryAndCache(projectId, sql, limit) {
    executing.value = true;
    error.value = null;
    arrowTable.value = null;
    columns.value = [];
    totalRows.value = 0;
    sortColumn.value = null;
    sortDirection.value = 'asc';
    filters.value = {};
    currentPage.value = 0;

    const startTime = Date.now();
    try {
      const response = await axios.post(
        `/api/projects/${projectId}/warehouse/query?format=arrow`,
        { sql, limit },
        {
          responseType: 'arraybuffer',
          headers: { Accept: ARROW_CONTENT_TYPE },
        }
      );
      executionTime.value = Date.now() - startTime;

      const table = tableFromIPC(new Uint8Array(response.data));
      arrowTable.value = table;
      totalRows.value = table.numRows;
      columns.value = table.schema.fields.map((f) => ({
        name: f.name,
        data_type: f.type.toString(),
        nullable: f.nullable,
      }));

      setCached(projectId, sql, response.data);
    } catch (e) {
      executionTime.value = Date.now() - startTime;
      if (e.response && e.response.data instanceof ArrayBuffer) {
        const text = new TextDecoder().decode(e.response.data);
        try {
          const json = JSON.parse(text);
          error.value = json.message || json.error || text;
        } catch {
          error.value = text;
        }
      } else {
        error.value =
          e.response?.data?.message || e.response?.data?.error || e.message;
      }
    } finally {
      executing.value = false;
    }
  }

  return {
    arrowTable,
    columns,
    executing,
    error,
    executionTime,
    totalRows,

    sortColumn,
    sortDirection,
    filters,
    currentPage,
    pageSize,

    processedResult,

    executeQuery,
    executeQueryCached,

    setSort,
    toggleSort,

    setFilter,
    removeFilter,
    clearFilters,

    setPage,
    nextPage,
    prevPage,
    setPageSize,

    aggregate,
    columnStats,

    exportCSV,
    exportJSON,
    downloadCSV,
    downloadJSON,
    downloadArrowIPC,
  };
}
