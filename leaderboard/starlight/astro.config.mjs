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
			],
		}),
	],
});
