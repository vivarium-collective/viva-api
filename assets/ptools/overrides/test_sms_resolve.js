const fs = require('fs');
const path = require('path');
// resolve relative to THIS file, not the cwd or an absolute developer path
const src = fs.readFileSync(path.join(__dirname, 'htdocs', 'sms', 'sms.js'), 'utf8');
// lift just the helper out of the file (no DOM deps)
const start = src.indexOf("const SMS_SCALE_SUFFIXES");
const end   = src.indexOf("/* Fetch a dataset from the simulation server */");
eval(src.slice(start, end));

let pass=0, fail=0;
const warns=[]; const _w=console.warn; console.warn=m=>warns.push(m);
function t(name, analysis, vt, expect) {
  const got = smsResolveValueType(analysis, vt);
  const ok = got === expect;
  ok ? pass++ : fail++;
  _w(`  ${ok?'PASS':'FAIL'}  ${name}\n        want=${JSON.stringify(expect)} got=${JSON.stringify(got)}`);
}

// 1. exact stem still wins (unchanged upstream behaviour)
t('exact key wins', {ptools_rna:'X', 'ptools_rna_multiseed__variant=0.tsv':'Y'}, 'ptools_rna', 'X');
// 2. unique prefix -- the 91/97 normal case
t('unique prefix', {'ptools_rna_multiseed__variant=0.tsv':'Y'}, 'ptools_rna', 'Y');
// .tsv and .html no longer collide -- and only the .tsv is eligible
t('tsv chosen over html for the same stem',
  {'ptools_rna_multiseed__variant=0.tsv':'DATA',
   'ptools_rna_multiseed__variant=0.html':'FIGURE'}, 'ptools_rna', 'DATA');
t('html-only -> undefined, never upload a figure as omics data',
  {'ptools_rna_multiseed__variant=0.html':'FIGURE'}, 'ptools_rna', undefined);
// 3. the REAL ambiguous case (Run-4 analysis-mnp-*): prefer no scale suffix
t('per-cell vs aggregate -> prefer the AGGREGATE (full time series)',
  {'ptools_overview__variant=0.tsv':'SINGLE', 'ptools_overview_multigeneration__variant=0.tsv':'MULTIGEN'},
  'ptools_overview', 'MULTIGEN');
// the real per-cell shape: many (gen, agent) files alongside one aggregate
t('many per-cell files + one aggregate -> the aggregate',
  {'ptools_rna__variant=0_seed=0_gen=0_agent=1.tsv':'G0',
   'ptools_rna__variant=0_seed=0_gen=1_agent=1.tsv':'G1',
   'ptools_rna_multigeneration__variant=0_seed=0.tsv':'LINEAGE'},
  'ptools_rna', 'LINEAGE');
// 4. nothing matches
t('miss returns undefined', {'ptools_rxns__variant=0.tsv':'Z'}, 'ptools_rna', undefined);
// 5. deterministic + warns when still ambiguous
const a5={'ptools_rna_multiseed__variant=0.tsv':'A','ptools_rna_multigeneration__variant=0.tsv':'B'};
t('two scales, no plain -> sorted first', a5, 'ptools_rna', 'B'); // multigeneration sorts before multiseed
// 6. falsy values ignored
t('falsy candidate skipped', {'ptools_rna_multiseed__variant=0.tsv':'', 'ptools_rna_x.tsv':'V'}, 'ptools_rna', 'V');
// 7. does not match a different view
t('prefix DOES match a longer view name (documented hazard, no real pair collides today)',
  {'ptools_rnap__variant=0.tsv':'NOPE'}, 'ptools_rna', 'NOPE');

console.warn = _w;
console.log(`\n  ambiguity warnings emitted: ${warns.length}`);
warns.forEach(w=>console.log('   ',w));
console.log(`\n  ${pass} passed, ${fail} failed`);
process.exit(fail?1:0);
