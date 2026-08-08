<template>
  <div class="space-y-8">
    <p class="text-sm text-gray-600 max-w-2xl">
      Pick servers and quantities. Throughput is the minimum of CPU (stress-test baseline scaled by
      total cores), disk (NVMe vs retention at your compression ratio), and RAM (rough cap per GB).
      Cost per TB uses that effective throughput against total monthly price.
    </p>

    <div>
      <h3 class="text-sm font-semibold text-gray-900 mb-3">Servers</h3>
      <div class="overflow-x-auto">
        <table class="min-w-full divide-y divide-gray-200 text-sm">
          <thead>
            <tr>
              <th class="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase w-20">Qty</th>
              <th class="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Model</th>
              <th class="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">CPU</th>
              <th class="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">RAM</th>
              <th class="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">NVMe</th>
              <th class="px-3 py-2 text-right text-xs font-medium text-gray-500 uppercase">€/mo</th>
              <th class="px-3 py-2 text-right text-xs font-medium text-gray-500 uppercase">Subtotal</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-gray-200">
            <tr v-for="s in hetznerServers" :key="s.id" :class="serverQty[s.id] > 0 ? 'bg-blue-50' : ''">
              <td class="px-3 py-2">
                <input
                  v-model.number="serverQty[s.id]"
                  type="number" min="0" max="20"
                  class="w-16 border-gray-300 rounded text-sm text-center"
                />
              </td>
              <td class="px-3 py-2 font-medium text-gray-900">{{ s.name }}</td>
              <td class="px-3 py-2 text-gray-500 text-xs">{{ s.cpu }}</td>
              <td class="px-3 py-2 text-gray-500">{{ s.ramGB }} GB</td>
              <td class="px-3 py-2 font-mono text-gray-500">{{ fmtTB(s.storageTB) }}</td>
              <td class="px-3 py-2 font-mono text-right">€{{ s.priceEur }}</td>
              <td class="px-3 py-2 font-mono text-right" :class="serverQty[s.id] > 0 ? 'font-semibold text-gray-900' : 'text-gray-300'">
                €{{ s.priceEur * serverQty[s.id] }}
              </td>
            </tr>
          </tbody>
          <tfoot>
            <tr class="border-t-2 border-gray-300">
              <td class="px-3 py-2 font-semibold text-gray-900">{{ clusterTotalQty }}</td>
              <td class="px-3 py-2 font-semibold text-gray-900" colspan="2">Cluster total</td>
              <td class="px-3 py-2 font-mono text-gray-700">{{ clusterTotals.ramGB }} GB</td>
              <td class="px-3 py-2 font-mono text-gray-700">{{ fmtTB(clusterTotals.storageTB) }}</td>
              <td class="px-3 py-2 font-mono text-right text-gray-500">{{ clusterTotals.cores }} cores</td>
              <td class="px-3 py-2 font-mono font-bold text-right text-gray-900">€{{ clusterTotals.priceEur }}/mo</td>
            </tr>
          </tfoot>
        </table>
      </div>
    </div>

    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
      <div>
        <label class="block text-sm font-medium text-gray-700 mb-1">Avg event size (bytes)</label>
        <input v-model.number="costEventSize" type="number" min="50" max="10000" step="50"
          class="w-full border-gray-300 rounded-md shadow-sm text-sm" />
        <p class="text-xs text-gray-400 mt-1">Typical: 300B logs, 500B spans, 100B metrics</p>
      </div>
      <div>
        <label class="block text-sm font-medium text-gray-700 mb-1">Retention (days)</label>
        <input v-model.number="costRetentionDays" type="number" min="1" max="365" step="1"
          class="w-full border-gray-300 rounded-md shadow-sm text-sm" />
        <p class="text-xs text-gray-400 mt-1">Disk limit assumes compressed data for this window</p>
      </div>
      <div>
        <label class="block text-sm font-medium text-gray-700 mb-1">Headroom (CPU)</label>
        <input v-model.number="costHeadroom" type="range" min="0.3" max="0.9" step="0.05"
          class="w-full accent-blue-600" />
        <p class="text-xs text-gray-400 mt-1">{{ (costHeadroom * 100).toFixed(0) }}% of CPU-derived peak</p>
      </div>
      <div>
        <label class="block text-sm font-medium text-gray-700 mb-1">ClickHouse compression</label>
        <input v-model.number="costCompression" type="range" min="3" max="20" step="1"
          class="w-full accent-blue-600" />
        <p class="text-xs text-gray-400 mt-1">{{ costCompression }}:1 on disk (affects disk cap)</p>
      </div>
    </div>

    <div v-if="clusterTotals.cores > 0">
      <p class="text-sm text-gray-700 mb-4">
        <span class="font-medium">Limiting factor:</span>
        <span :class="limitingFactor === 'disk' ? 'text-amber-700 font-medium' : limitingFactor === 'ram' ? 'text-purple-700 font-medium' : 'text-blue-700 font-medium'">
          {{ limitingFactorLabel }}
        </span>
      </p>
      <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6 text-xs text-gray-600">
        <div class="bg-gray-50 border border-gray-200 rounded p-3">
          <span class="font-medium text-gray-800">CPU ceiling</span>
          <p class="font-mono mt-1">{{ fmtDataRate(cpuBytesPerSec) }}/s · {{ fmtTB(cpuRawTBPerMonth) }}/mo raw</p>
        </div>
        <div class="bg-gray-50 border border-gray-200 rounded p-3">
          <span class="font-medium text-gray-800">Disk ceiling</span>
          <p class="font-mono mt-1">{{ fmtDataRate(diskCapBytesPerSec) }}/s · {{ fmtTB(diskCapRawTBPerMonth) }}/mo raw</p>
          <p class="text-gray-400 mt-1">≤ {{ fmtTB(compressedFootprintTB) }} compressed in {{ costRetentionDays }}d vs {{ fmtTB(clusterTotals.storageTB) }} NVMe</p>
        </div>
        <div class="bg-gray-50 border border-gray-200 rounded p-3">
          <span class="font-medium text-gray-800">RAM ceiling</span>
          <p class="font-mono mt-1">{{ fmtDataRate(ramCapBytesPerSec) }}/s · {{ fmtTB(ramCapRawTBPerMonth) }}/mo raw</p>
          <p class="text-gray-400 mt-1">~{{ RAM_MB_PER_GB_RAM }} MB/s per GB cluster RAM (heuristic)</p>
        </div>
      </div>
      <div class="grid grid-cols-2 gap-4 mb-8">
        <div class="bg-blue-50 border border-blue-200 rounded-lg p-5">
          <p class="text-xs text-blue-600 font-medium uppercase tracking-wide">Effective sustained throughput</p>
          <p class="text-3xl font-bold text-blue-900 mt-2">{{ fmtDataRate(sustainedBytesPerSec) }}/s</p>
          <p class="text-sm text-blue-600 mt-1">{{ fmtNum(sustainedEventsPerSec) }} events/s · {{ fmtTB(rawTBPerMonth) }} raw / month</p>
        </div>
        <div class="bg-green-50 border border-green-200 rounded-lg p-5">
          <p class="text-xs text-green-600 font-medium uppercase tracking-wide">Your cost per TB ingested</p>
          <p class="text-3xl font-bold text-green-900 mt-2">€{{ costPerTB }}</p>
          <p class="text-sm text-green-600 mt-1">€{{ clusterTotals.priceEur }}/mo ÷ {{ fmtTB(rawTBPerMonth) }} raw / mo</p>
        </div>
      </div>

      <h3 class="text-sm font-semibold text-gray-900 mb-3">Market comparison (per TB ingested)</h3>
      <div class="overflow-x-auto mb-6">
        <table class="min-w-full divide-y divide-gray-200 text-sm">
          <thead>
            <tr>
              <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Provider</th>
              <th class="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Price / TB</th>
              <th class="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">vs. your cost</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-gray-200">
            <tr v-for="c in competitors" :key="c.name">
              <td class="px-4 py-3 text-gray-900">{{ c.name }}</td>
              <td class="px-4 py-3 font-mono text-right">€{{ c.pricePerTB }}</td>
              <td class="px-4 py-3 font-mono text-right text-green-600 font-medium">{{ costPerTBNum > 0 ? Math.round(c.pricePerTB / costPerTBNum) + 'x cheaper' : '-' }}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <h3 class="text-sm font-semibold text-gray-900 mb-3">Suggested pricing tiers</h3>
      <div class="overflow-x-auto">
        <table class="min-w-full divide-y divide-gray-200 text-sm">
          <thead>
            <tr>
              <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Tier</th>
              <th class="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Charge / TB</th>
              <th class="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Margin</th>
              <th class="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Break-even</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-gray-200">
            <tr v-for="t in pricingTiers" :key="t.name">
              <td class="px-4 py-3 font-medium text-gray-900">{{ t.name }}</td>
              <td class="px-4 py-3 font-mono text-right">€{{ t.pricePerTB.toFixed(2) }}</td>
              <td class="px-4 py-3 text-right">
                <span :class="t.margin > 95 ? 'text-green-600 font-medium' : t.margin > 80 ? 'text-blue-600' : 'text-gray-600'">
                  {{ t.margin.toFixed(1) }}%
                </span>
              </td>
              <td class="px-4 py-3 font-mono text-right text-gray-500">{{ t.breakEvenTB }} TB/mo</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div v-else class="text-sm text-gray-400 py-8 text-center">
      Set quantity on at least one server to see estimates.
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, computed } from 'vue';

const BASELINE_CORES = 12;
const BASELINE_EVENTS_PER_SEC = 25000;
const BASELINE_EVENT_SIZE = 5000;

const hetznerServers = [
  { id: 'ex44',     name: 'EX44',           cpu: 'i5-13500 (14c/20t)',           cores: 14,  ramGB: 64,   storageTB: 1.0,   priceEur: 44  },
  { id: 'ax42u',    name: 'AX42-U',         cpu: 'Ryzen 7 PRO 8700GE (8c/16t)', cores: 8,   ramGB: 64,   storageTB: 1.0,   priceEur: 54  },
  { id: 'ax102u',   name: 'AX102-U',        cpu: 'Ryzen 9 7950X3D (16c/32t)',   cores: 16,  ramGB: 128,  storageTB: 3.84,  priceEur: 119 },
  { id: 'ex44-8tb', name: 'EX44 + 7.68TB',  cpu: 'i5-13500 (14c/20t)',          cores: 14,  ramGB: 64,   storageTB: 8.7,   priceEur: 142 },
  { id: 'ex130r',   name: 'EX130-R',        cpu: 'Xeon Gold 5412U (24c/48t)',   cores: 24,  ramGB: 256,  storageTB: 3.84,  priceEur: 154 },
  { id: 'ex130s',   name: 'EX130-S',        cpu: 'Xeon Gold 5412U (24c/48t)',   cores: 24,  ramGB: 128,  storageTB: 7.68,  priceEur: 154 },
  { id: 'ex63',     name: 'EX63 (30TB)',     cpu: 'Ultra 7 265 (20c/20t)',       cores: 20,  ramGB: 64,   storageTB: 30.72, priceEur: 213 },
  { id: 'ax162r',   name: 'AX162-R',        cpu: 'EPYC 9454P (48c/96t)',        cores: 48,  ramGB: 256,  storageTB: 3.84,  priceEur: 229 },
  { id: 'ax162s',   name: 'AX162-S',        cpu: 'EPYC 9454P (48c/96t)',        cores: 48,  ramGB: 128,  storageTB: 7.68,  priceEur: 229 },
  { id: 'ex44-16tb', name: 'EX44 + 2x7.68TB', cpu: 'i5-13500 (14c/20t)',        cores: 14,  ramGB: 64,   storageTB: 16.36, priceEur: 230 },
];

const competitors = [
  { name: 'Datadog (Logs)',    pricePerTB: 100 },
  { name: 'New Relic',         pricePerTB: 300 },
  { name: 'Grafana Cloud',     pricePerTB: 500 },
  { name: 'Elastic Cloud',     pricePerTB: 250 },
];

const serverQty = reactive(Object.fromEntries(hetznerServers.map((s) => [s.id, 0])));
const costEventSize = ref(350);
const costRetentionDays = ref(30);
const costHeadroom = ref(0.7);
const costCompression = ref(10);

const RAM_MB_PER_GB_RAM = 2;

const clusterTotalQty = computed(() => hetznerServers.reduce((sum, s) => sum + (serverQty[s.id] || 0), 0));

const clusterTotals = computed(() => {
  let cores = 0, ramGB = 0, storageTB = 0, priceEur = 0;
  for (const s of hetznerServers) {
    const qty = serverQty[s.id] || 0;
    cores += s.cores * qty;
    ramGB += s.ramGB * qty;
    storageTB += s.storageTB * qty;
    priceEur += s.priceEur * qty;
  }
  return { cores, ramGB, storageTB, priceEur };
});

const cpuEventsPerSec = computed(() => {
  const { cores } = clusterTotals.value;
  if (cores <= 0) return 0;
  const coreRatio = cores / BASELINE_CORES;
  const sizeRatio = BASELINE_EVENT_SIZE / Math.max(50, costEventSize.value);
  const scaleFactor = Math.min(coreRatio, sizeRatio) * Math.sqrt(Math.max(coreRatio, sizeRatio));
  return Math.round(BASELINE_EVENTS_PER_SEC * scaleFactor * costHeadroom.value);
});

const cpuBytesPerSec = computed(() => cpuEventsPerSec.value * costEventSize.value);

const cpuRawTBPerMonth = computed(() => (cpuBytesPerSec.value * 86400 * 30) / 1e12);

const diskCapRawTBPerMonth = computed(() => {
  const S = clusterTotals.value.storageTB;
  const C = Math.max(3, costCompression.value);
  const R = Math.max(1, costRetentionDays.value);
  if (S <= 0) return Infinity;
  return (S * 30 * C) / R;
});

const diskCapBytesPerSec = computed(() => {
  const m = diskCapRawTBPerMonth.value;
  if (!Number.isFinite(m) || m <= 0) return 0;
  return (m * 1e12) / (86400 * 30);
});

const ramCapBytesPerSec = computed(() => {
  const ram = clusterTotals.value.ramGB;
  if (ram <= 0) return 0;
  return ram * RAM_MB_PER_GB_RAM * 1e6;
});

const ramCapRawTBPerMonth = computed(() => (ramCapBytesPerSec.value * 86400 * 30) / 1e12);

const sustainedBytesPerSec = computed(() => {
  const cpu = cpuBytesPerSec.value;
  const disk = diskCapBytesPerSec.value;
  const ram = ramCapBytesPerSec.value;
  const diskLim = Number.isFinite(diskCapRawTBPerMonth.value) && diskCapRawTBPerMonth.value > 0 ? disk : Infinity;
  return Math.min(cpu, diskLim, ram);
});

const sustainedEventsPerSec = computed(() => {
  const es = Math.floor(sustainedBytesPerSec.value / Math.max(1, costEventSize.value));
  return Math.max(0, es);
});

const rawTBPerMonth = computed(() => (sustainedBytesPerSec.value * 86400 * 30) / 1e12);

const compressedFootprintTB = computed(() => {
  const raw = rawTBPerMonth.value;
  const C = Math.max(3, costCompression.value);
  const R = Math.max(1, costRetentionDays.value);
  return (raw * (R / 30)) / C;
});

const limitingFactor = computed(() => {
  const cpu = cpuBytesPerSec.value;
  const diskBps = Number.isFinite(diskCapRawTBPerMonth.value) && diskCapRawTBPerMonth.value > 0
    ? diskCapBytesPerSec.value
    : Infinity;
  const ram = ramCapBytesPerSec.value;
  const m = Math.min(cpu, diskBps, ram);
  const eps = 1e-6;
  if (Math.abs(m - cpu) < eps) return 'cpu';
  if (Math.abs(m - diskBps) < eps) return 'disk';
  return 'ram';
});

const limitingFactorLabel = computed(() => {
  switch (limitingFactor.value) {
    case 'cpu':
      return 'CPU (baseline scaled by total cores)';
    case 'disk':
      return 'NVMe (retention + compression vs cluster storage)';
    default:
      return 'RAM (heuristic per GB)';
  }
});

const costPerTBNum = computed(() => {
  const tb = rawTBPerMonth.value;
  if (tb <= 0) return 0;
  return clusterTotals.value.priceEur / tb;
});

const costPerTB = computed(() => costPerTBNum.value.toFixed(2));

const pricingTiers = computed(() => {
  const cost = costPerTBNum.value;
  const totalPrice = clusterTotals.value.priceEur;
  return [
    { name: 'Enterprise', pricePerTB: Math.max(cost * 30, 10) },
    { name: 'Business',   pricePerTB: Math.max(cost * 80, 30) },
    { name: 'Pro',        pricePerTB: Math.max(cost * 150, 50) },
  ].map((t) => ({
    ...t,
    margin: t.pricePerTB > 0 ? ((t.pricePerTB - cost) / t.pricePerTB) * 100 : 0,
    breakEvenTB: t.pricePerTB > 0 ? (totalPrice / t.pricePerTB).toFixed(2) : '0',
  }));
});

function fmtNum(n) {
  if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return String(n);
}

function fmtTB(tb) {
  if (tb >= 1) return tb.toFixed(1) + ' TB';
  return (tb * 1000).toFixed(0) + ' GB';
}

function fmtDataRate(bytesPerSec) {
  if (bytesPerSec >= 1e9) return (bytesPerSec / 1e9).toFixed(1) + ' GB';
  if (bytesPerSec >= 1e6) return (bytesPerSec / 1e6).toFixed(1) + ' MB';
  if (bytesPerSec >= 1e3) return (bytesPerSec / 1e3).toFixed(0) + ' KB';
  return bytesPerSec + ' B';
}
</script>
