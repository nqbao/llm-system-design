import fs from 'node:fs';

import DOMPurify from './starlight/node_modules/dompurify/dist/purify.es.mjs';
import { diagram as classDiagram } from './starlight/node_modules/mermaid/dist/chunks/mermaid.core/classDiagram-4FO5ZUOK.mjs';
import { diagram as erDiagram } from './starlight/node_modules/mermaid/dist/chunks/mermaid.core/erDiagram-TEJ5UH35.mjs';
import { diagram as flowDiagram } from './starlight/node_modules/mermaid/dist/chunks/mermaid.core/flowDiagram-I6XJVG4X.mjs';
import { diagram as ganttDiagram } from './starlight/node_modules/mermaid/dist/chunks/mermaid.core/ganttDiagram-6RSMTGT7.mjs';
import { diagram as journeyDiagram } from './starlight/node_modules/mermaid/dist/chunks/mermaid.core/journeyDiagram-JHISSGLW.mjs';
import { diagram as sequenceDiagram } from './starlight/node_modules/mermaid/dist/chunks/mermaid.core/sequenceDiagram-3UESZ5HK.mjs';
import { diagram as stateDiagram } from './starlight/node_modules/mermaid/dist/chunks/mermaid.core/stateDiagram-v2-BHNVJYJU.mjs';

DOMPurify.addHook = () => {};
DOMPurify.sanitize = (value) => value;

const MERMAID_FENCE_RE = /```mermaid\s*\n([\s\S]*?)```/g;

const PARSERS = {
  classdiagram: classDiagram,
  erdiagram: erDiagram,
  flowchart: flowDiagram,
  gantt: ganttDiagram,
  graph: flowDiagram,
  journey: journeyDiagram,
  sequencediagram: sequenceDiagram,
  'statediagram-v2': stateDiagram,
};

function diagramType(block) {
  for (const rawLine of block.split('\n')) {
    const line = rawLine.trim();
    if (!line || line.startsWith('%%')) {
      continue;
    }
    return line.split(/\s+/, 1)[0].toLowerCase();
  }
  return null;
}

function isInvalid(block) {
  const type = diagramType(block);
  const diagram = type ? PARSERS[type] : null;
  if (!diagram) {
    return false;
  }

  const db = diagram.db;
  const parser = diagram.parser;
  parser.parser.yy = db;
  try {
    parser.parse(block);
    return false;
  } catch {
    return true;
  }
}

const path = process.argv[2];
if (!path) {
  console.error('usage: node validate_mermaid.mjs <markdown-path>');
  process.exit(2);
}

const text = fs.readFileSync(path, 'utf8');
const result = [];
for (const match of text.matchAll(MERMAID_FENCE_RE)) {
  result.push(isInvalid(match[1]));
}

process.stdout.write(`${JSON.stringify(result)}\n`);
