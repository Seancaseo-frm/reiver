<template>
  <div class="min-h-screen bg-paper text-ink antialiased font-body">
    <MarketingNav />
    <main>
      <MarketingHero />
      <TrustStrip />
      <DifferentiatorsGrid />
      <ProductExplorer />
      <OpenStandardsSection />
      <PricingTeaser />
      <FinalCta />
    </main>
    <MarketingFooter />
  </div>
</template>

<script setup>
import MarketingNav from './Home/MarketingNav.vue';
import MarketingHero from './Home/MarketingHero.vue';
import TrustStrip from './Home/TrustStrip.vue';
import DifferentiatorsGrid from './Home/DifferentiatorsGrid.vue';
import ProductExplorer from './Home/ProductExplorer.vue';
import OpenStandardsSection from './Home/OpenStandardsSection.vue';
import PricingTeaser from './Home/PricingTeaser.vue';
import FinalCta from './Home/FinalCta.vue';
import MarketingFooter from './Home/MarketingFooter.vue';
</script>
