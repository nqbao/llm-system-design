// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import mermaid from 'astro-mermaid';
import { sidebar } from './src/sidebar.gen.js';

// https://astro.build/config
const base = process.env.BASE_PATH ?? '';

export default defineConfig({
	site: 'https://nqbao.github.io',
	base,
	outDir: '../dist',
	integrations: [
		mermaid(),
		starlight({
			title: 'LLM System Design Benchmark',
			description: 'Comparing how different LLMs perform on system design questions',
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/nqbao/llm-system-design' },
			],
			customCss: [
				'./src/styles/custom.css',
			],
			tableOfContents: false,
			sidebar,
			head: [
				{
					tag: 'script',
					attrs: {
						'data-goatcounter': 'https://nqbao.goatcounter.com/count',
						async: true,
						src: '//gc.zgo.at/count.js',
					},
				},
				{
					tag: 'script',
					attrs: { type: 'text/javascript' },
					content: `
function fixMermaidErrors() {
  const els = document.querySelectorAll('pre.mermaid[data-processed]');
  for (const el of els) {
    const svg = el.querySelector('svg[aria-roledescription="error"]');
    if (svg) {
      const raw = el.getAttribute('data-diagram') || el.textContent;
      el.removeAttribute('data-processed');
      el.innerHTML = '<pre style="background:var(--sl-color-gray-6);padding:1rem;border-radius:0.5rem;overflow:auto"><code>' +
        raw.replace(/[&<>]/g, function(c) { return {'&':'&amp;','<':'&lt;','>':'&gt;'}[c]; }) +
        '</code></pre>';
    }
  }
}
const mo = new MutationObserver(function() { fixMermaidErrors(); });
mo.observe(document.documentElement, { subtree: true, attributes: true, attributeFilter: ['data-processed'] });
document.addEventListener('astro:after-swap', function() { setTimeout(fixMermaidErrors, 500); });
fixMermaidErrors();
					`.replace(/^\t{5}/gm, ''),
				},
			],
		}),
	],
});
