const fs = require('fs');
const path = require('path');
// resolve relative to THIS file, not the cwd or an absolute developer path
const src = fs.readFileSync(path.join(__dirname, 'htdocs', 'sms', 'sms.js'), 'utf8');
// lift just the function under test out of the file (needs a DOM, stubbed below)
const start = src.indexOf("function populateAnalysisSelector");
const end   = src.indexOf("/* This is obsolete.");
eval(src.slice(start, end));

// --- minimal DOM stub -------------------------------------------------------
// Just enough surface (classList/appendChild/innerHTML/select.add) for this
// one function. Real bug this catches: a bare `createElement(...)` call
// (missing the `document.` prefix) throws ReferenceError under a real DOM,
// which this stub reproduces by NOT defining a global `createElement`.
class FakeElement {
  constructor(tag) { this.tag = tag; this.children = []; this.classList = { add: () => {} }; }
  appendChild(child) { this.children.push(child); return child; }
  replaceChildren() { this.children = []; }
  add(option) { this.children.push(option); } // <select>.add(option)
  set innerHTML(v) { this._html = v; }
  get innerHTML() { return this._html; }
}
global.document = { createElement: (tag) => new FakeElement(tag) };
global.Option = class { constructor(text, value) { this.text = text; this.value = value; } };

let pass = 0, fail = 0;
function t(name, fn) {
  try {
    fn();
    pass++;
    console.log(`  PASS  ${name}`);
  } catch (e) {
    fail++;
    console.log(`  FAIL  ${name}\n        ${e.stack}`);
  }
}

// The exact trigger: 2+ analyses hits the `else` branch (single-analysis
// selection uses the already-correct `document.createElement` path).
t('multiple analyses: does not throw, builds a <select> with one option per analysis', () => {
  const parent = new FakeElement('div');
  const analyses = [
    { database_id: 1, n_tp: 10, experiment_id: 'expt-a' },
    { database_id: 2, n_tp: 20, experiment_id: 'expt-a' },
  ];
  populateAnalysisSelector(analyses, parent);
  const label = parent.children.find(c => c.tag === 'label');
  if (!label) throw new Error('expected a <label> to be appended');
  const select = label.children.find(c => c.tag === 'select');
  if (!select) throw new Error('expected a <select> inside the label');
  if (select.children.length !== 2) throw new Error(`expected 2 options, got ${select.children.length}`);
});

// Regression guard on the branch that already worked, so the fix doesn't
// accidentally change single-analysis behavior.
t('single analysis: hidden input path still works', () => {
  const parent = new FakeElement('div');
  populateAnalysisSelector([{ database_id: 1, n_tp: 10, experiment_id: 'expt-a' }], parent);
  const input = parent.children.find(c => c.tag === 'input');
  if (!input) throw new Error('expected a hidden <input> to be appended');
});

t('no analyses: shows the "no data" message, does not throw', () => {
  const parent = new FakeElement('div');
  populateAnalysisSelector([], parent);
  if (!parent.innerHTML) throw new Error('expected an innerHTML message');
});

console.log(`\n  ${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
