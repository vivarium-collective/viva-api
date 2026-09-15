/*
 * Regression test for the multi-analysis branch of populateAnalysisSelector().
 *
 * Upstream sms.js (aic-export-30.0, vendored verbatim in c337f35b) calls a bare
 * `createElement(...)` three times in the `analyses.length > 1` branch, while the
 * other 27 call sites in the same file -- including the single-analysis branch of
 * this very function -- correctly say `document.createElement(...)`. No global
 * `createElement` is defined anywhere in the PTools htdocs tree, so that branch
 * threw `ReferenceError: createElement is not defined` and the analysis selector
 * never rendered.
 *
 * That branch is not exotic: `analysisDesc()` renders "Number of intervals: n_tp",
 * i.e. the selector exists precisely so a user can choose among several analyses of
 * one experiment that differ by n_tp -- which is the exact shape of the backfilled
 * CD2 analyses (n_tp 5 / 10 / 20).
 *
 * This test runs the function for real against a minimal DOM stub rather than
 * grepping for the string, so it also proves the branch builds the <select>.
 */
const fs = require('fs');
const path = require('path');
const src = fs.readFileSync(path.join(__dirname, 'htdocs', 'sms', 'sms.js'), 'utf8');

/* ---- minimal DOM, only what populateAnalysisSelector touches ---- */
function makeEl (tag) {
  return {
    tagName: tag.toUpperCase(),
    children: [],
    options: [],
    classList: { _c: [], add (c) { this._c.push(c); }, contains (c) { return this._c.includes(c); } },
    innerHTML: '',
    appendChild (c) { this.children.push(c); return c; },
    replaceChildren () { this.children = []; },
    add (opt) { this.options.push(opt); },
  };
}
global.document = { createElement: makeEl, getElementById: () => makeEl('div') };
global.Option = function (text, value) { return { text, value }; };

/* lift just this function out of the file (it closes over nothing else) */
const start = src.indexOf('function populateAnalysisSelector (analyses, parent) {');
const end = src.indexOf('/* This is obsolete.');
if (start < 0 || end < 0 || end <= start) {
  console.log('  FAIL  could not locate populateAnalysisSelector in sms.js');
  process.exit(1);
}
eval(src.slice(start, end));

let pass = 0, fail = 0;
function check (name, fn) {
  let ok = false, err = '';
  try { ok = fn() === true; } catch (e) { err = ` threw ${e.name}: ${e.message}`; }
  ok ? pass++ : fail++;
  console.log(`  ${ok ? 'PASS' : 'FAIL'}  ${name}${err}`);
}

const A = [
  { database_id: 11, n_tp: 5, experiment_id: 'exp-1' },
  { database_id: 12, n_tp: 10, experiment_id: 'exp-1' },
  { database_id: 13, n_tp: 20, experiment_id: 'exp-1' },
];

/* 1. THE REGRESSION: >1 analysis must not throw, and must build a real <select>. */
check('multi-analysis branch renders a <select> with one option per analysis', () => {
  const parent = makeEl('div');
  populateAnalysisSelector(A, parent);
  const label = parent.children.find(c => c.tagName === 'LABEL');
  if (!label) return false;
  const select = label.children.find(c => c.tagName === 'SELECT');
  return !!select && select.options.length === A.length && select.classList.contains('analysisId');
});

/* 2. the hidden experimentId input is appended in the same call */
check('multi-analysis branch still appends the hidden experimentId input', () => {
  const parent = makeEl('div');
  populateAnalysisSelector(A, parent);
  const hidden = parent.children.filter(c => c.tagName === 'INPUT' && c.classList.contains('experimentId'));
  return hidden.length === 1 && hidden[0].value === 'exp-1';
});

/* 3. single-analysis branch unchanged (it was always correct) */
check('single-analysis branch renders a hidden analysisId input, no <select>', () => {
  const parent = makeEl('div');
  populateAnalysisSelector([A[0]], parent);
  const input = parent.children.find(c => c.tagName === 'INPUT' && c.classList.contains('analysisId'));
  const select = parent.children.find(c => c.tagName === 'SELECT');
  return !!input && input.value === 11 && !select;
});

/* 4. empty list is a message, not a crash */
check('no analyses -> message, no throw', () => {
  const parent = makeEl('div');
  populateAnalysisSelector([], parent);
  return /no analysis data available/i.test(parent.innerHTML);
});

/* 5. belt-and-braces: no bare createElement( survives anywhere in the file */
check('no bare createElement( anywhere in sms.js', () => {
  const bare = src.split('\n')
    .map((l, i) => [i + 1, l])
    .filter(([, l]) => /(^|[^.\w$])createElement *\(/.test(l));
  if (bare.length) console.log('        offending lines: ' + bare.map(([n]) => n).join(', '));
  return bare.length === 0;
});

console.log(`\n  ${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
