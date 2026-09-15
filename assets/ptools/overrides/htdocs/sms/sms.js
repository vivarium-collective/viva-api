var $j = jQuery.noConflict();

const ptoolsBaseUrl = "";
const defaultOrgid = 'ECOLI';
let linkTarget = '_blank'; // where links in embedded divs should open.

/* An array of objects that links a SMS simulation id (and other parameters) to
   a PTools registered dataset key. This data is cached in localStorage. */
let cachedDatasets = initCachedDatasets();

/* An array of objects describing each simulation on the server, including tags
   and, if already retrieved, analysis descriptions. */
let allSimulations = [];

function initCachedDatasets () {
  const prevCachedDatasets = JSON.parse(localStorage.getItem('smsDatasets'));
  if (prevCachedDatasets) {
    const allCachedDatakeys = JSON.parse(localStorage.getItem('omicsDatasets'))?.map(x=>x.key) || [];
    const cachedDatasets = {};
    let changed = false;
    for (ds in prevCachedDatasets) {
      if (allCachedDatakeys.includes(prevCachedDatasets[ds].key))
	cachedDatasets[ds] = prevCachedDatasets[ds];
      else changed = true;
    }
    if (changed)
      localStorage.setItem('smsDatasets', JSON.stringify(cachedDatasets));
    return cachedDatasets;
  }
  else return {};
}

function cacheDataset (cacheKey, datasetInfo) {
  cachedDatasets[cacheKey] = datasetInfo;
  localStorage.setItem('smsDatasets', JSON.stringify(cachedDatasets));
}

function clearCachedDatasets() {
  cachedDatasets = {};
  localStorage.removeItem('smsDatasets');
}

function orgId () {
  return document.getElementById('orgSelect').value || defaultOrgid;
}

/* Given params specifying a simulation dataset, if there is already a
   corresponding PTools registered dataset in cache, just return its info.
   Otherwise, fetch the simulation data from the server, upload it
   to PTools as a registered dataset, cache the resulting correspondence, and
   return the params with the dataset key.
 */
async function fetchAndRegisterSimData (params, reloadP) {
  const simParams = new URLSearchParams(params);
  const cacheKey = simParams.toString()
  const cachedDataset = !reloadP && await getCachedDataset(cacheKey);
  if (cachedDataset) return cachedDataset;
  const rawData = await fetchDatasetSim(params);
  if (rawData) {
    const ptoolsKey = await registerDatasetPtools(params, rawData);
    if (ptoolsKey) {
      const datasetInfo = { key: ptoolsKey,
			    params: params
			  };
      cacheDataset(cacheKey, datasetInfo);
      return datasetInfo;
    }
  }
}
    

/* Given a cacheKey (a string derived from a params object), see if the PTools
   dataset has been previously cached. If so, make sure that it is valid on
   the server, reuploading as necessary. If not return false.
 */
async function getCachedDataset (cacheKey) {
  const datasetInfo = cachedDatasets[cacheKey];
  if (datasetInfo && datasetInfo.key) {
    try {
      const cached = await validateOmics(datasetInfo.key);
      if (cached) return datasetInfo;
    }
    catch (error)  { return false; }
  }
  return false;
}

/* Given simulation data, generate the params needed to upload to PTools from
   the sim params, and register the dataset. Return the generated dataset key.
 */
async function registerDatasetPtools (params, data) {
  const msgCounter = showWorkingMsg("Uploading data to PathwayTools...");
  const sim = getSim(params.experiment_id);
  let title = sim?.name || params.experiment_id;
  const ptoolsParams = new FormData();
  if (params.valueType == 'ptools_rna') {
    ptoolsParams.append('class', 'gene');
    title += " RNA Counts";
  }
  else if (params.valueType == 'ptools_proteins') {
    ptoolsParams.append('class', 'protein');
    title += " Protein Counts";
  }
  else if (params.valueType == 'ptools_rxns') {
    ptoolsParams.append('class', 'reaction');
    title += " Reaction Fluxes";
  }
  ptoolsParams.append("orgid", orgId());
  ptoolsParams.append("expressiontype", "absolute");
  ptoolsParams.append("datacolumns", getDataColumns(params));
  ptoolsParams.append("datatext", data);
  ptoolsParams.append("title", title);
  const ptoolsUrl = `${ptoolsBaseUrl}/register-omics-dataset`;
  try {
    const ptoolsResponse = await fetch(ptoolsUrl, {
      method: 'POST',
      body: ptoolsParams
    });
    if (!ptoolsResponse.ok)
      throw new Error("Visualization network response was not ok");
    const response = await ptoolsResponse.json();
    if (!response.success)
      throw new Error(response.error);
    saveOmics(ptoolsParams, response);
    hideWorkingMsg(msgCounter);
    return response.key;
  }
  catch (error) {
    hideWorkingMsg(msgCounter);
    console.error("Upload of data to PTools failed:", error);
  }

}

/* --- sms-api patch: tolerant valueType resolution -------------------------------
   Upstream keys its cache on `filename.split('.')[0]` -- the exact filename stem --
   then looks up `analysis[params.valueType]`. The simulation server's ptools analyses
   land as `<view>_<scale>__variant=<n>.tsv` (e.g. `ptools_rna_multiseed__variant=0.tsv`),
   so the stem is never equal to the requested valueType ("ptools_rna") and every
   aggregate file is unreachable. See CovertLabEcoli/sms-ecoli#166.

   Resolution order:
     1. exact stem                  -- unchanged behaviour; wins whenever present
     2. unique prefix match
     3. several prefix matches      -- prefer the candidate with no scale suffix
     4. still ambiguous             -- first by sort order, and warn (never silent)

   Step 3 is not hypothetical: 6 of 97 bundles in the live bucket carry two scales for
   one view type inside one analysis (the Run-4 `analysis-mnp-*` directories, where a
   fill appended alongside sim-time output). Without it the winner is whichever key the
   forEach assigned last, which is order-dependent and gives no sign it happened.
*/
const SMS_SCALE_SUFFIXES = ['_multiseed', '_multigeneration'];

function smsResolveValueType (analysis, valueType) {
  if (!analysis || !valueType) return undefined;
  if (analysis[valueType]) return analysis[valueType];            // 1
  const hits = Object.keys(analysis).filter(
    k => k !== valueType && k.startsWith(valueType) && analysis[k]);
  if (hits.length === 0) return undefined;
  if (hits.length === 1) return analysis[hits[0]];                // 2
  const plain = hits.filter(
    k => !SMS_SCALE_SUFFIXES.some(sfx => k.slice(valueType.length).startsWith(sfx)));
  if (plain.length === 1) return analysis[plain[0]];              // 3
  const pick = (plain.length ? plain : hits).slice().sort()[0];   // 4
  console.warn(`sms: valueType "${valueType}" is ambiguous (${hits.join(', ')}); using "${pick}"`);
  return analysis[pick];
}

/* Fetch a dataset from the simulation server */
async function fetchDatasetSim (params, retries) {
  if (!retries) retries = 0;
  const maxRetries = 5;
  const sim = getSim(params.experiment_id);
  const analysis = sim.analyses.find(x=>x.database_id==params.analysis_id);
  const cachedHit = smsResolveValueType(analysis, params.valueType);
  if (cachedHit) return cachedHit;
  const msgCounter = showWorkingMsg("Fetching simulation data...");
  try {
    const response = await fetch(`${simBaseUrl}analyses/${params.analysis_id}/data`, {
    });
    hideWorkingMsg(msgCounter);
    if (!response.ok)
      throw new Error (`HTTP error; status: ${response.status}`);
    const data = await response.json();
    if (!data[0]?.content)
      throw new Error(`Error fetching simulation data: ${data.detail[0].msg}`);
    data.forEach(entry=>{
      const vtype = entry.filename.split('.')[0];
      const content = entry.content;
      if (vtype && content) analysis[vtype] = content;
    });
  }
  catch (error) {
    hideWorkingMsg(msgCounter);
    if (retries < maxRetries)
      return fetchDatasetSim(params, retries+1);
    else {
      console.error("Error fetching simulation data", error);
    }
  }
  const fetchedHit = smsResolveValueType(analysis, params.valueType);
  if (fetchedHit) return fetchedHit;
  else console.error(`${params.valueType} missing from analysis results.`);
}

/* Fetch multiple datasets from the simulation server. These are all for a
   single analysis of the same simulation, but can include multiple data types
   (e.g. proteins, RNAs, rxn fluxes).
 */
async function fetchAndRegisterAllSimData (valueTypes, params, reloadP) {
  const msgCounter = showWorkingMsg();
  const datasets = [];
  // paley:Apr-6-2026 We have to fetch datasets sequentially, not in parallel.
  for (let valueType of valueTypes) {
    const clonedParams = { ...params };
    clonedParams.valueType = valueType;
    const dataset = await fetchAndRegisterSimData(clonedParams, reloadP);
    if (dataset) datasets.push(dataset);
  }
  hideWorkingMsg(msgCounter);
  return datasets;
}

/* Return the value to supply as the column1 parameter for PTools, based on the
   number of timepoints in a simulation analysis.
*/
function getDataColumns (params) {
  const sim = getSim(params.experiment_id);
  const analysis = sim.analyses.find(x=>x.database_id==params.analysis_id);
  if (analysis.n_tp > 1)
    return "1-" + analysis.n_tp;
  else return "1";
}

let vizCounter = 0;

function generateIframeViz(description, width, height) {
  let allVizContainer = document.getElementById("visualizations");
  let vizDiv = document.createElement('div');
  vizDiv.classList.add("iframeViz");
  let vizControls = document.createElement('div');
  vizControls.classList.add("vizControls");
  vizDiv.appendChild(vizControls);
  let iframeName = 'vizFrame' + vizCounter++;
  let iframe = getOrCreateIFrame(iframeName, vizDiv, width, height);
  let overlay = document.createElement('div');
  overlay.title = description;
  overlay.classList.add("thumbnailOverlay");
  vizDiv.appendChild(overlay);
  overlay.addEventListener('click', ()=>{
    if (vizDiv.classList.contains('thumbnail'))
      expandIframeViz(vizDiv);
  });
  function addControlIcon (faClass, handler, ttip) {
    let icon = document.createElement('i');
    icon.className = faClass;
    if (ttip) icon.title = ttip;
    vizControls.appendChild(icon);
    icon.addEventListener('click', handler);
  }
  addControlIcon("fa fa-clone", ()=>{
    window.open(iframe.src, "_blank");
  },
		 "Open in new tab");
  addControlIcon("fa fa-window-minimize", ()=>{
    collapseIframeViz(vizDiv, description);
  },
		 "Collapse to thumbnail");
  addControlIcon("fa fa-close", ()=>{
    vizDiv.remove();
  },
		 "Delete");
  allVizContainer.prepend(vizDiv);
  return iframe;
}

function getOrCreateIFrame (iframeName, container, width, height) {
  let iframe = document.querySelector(`iframe[name="${iframeName}"]`);

  // Create iframe if it doesn't exist
  if (!iframe) {
    iframe = document.createElement('iframe');
    iframe.name = iframeName;
    setIframeVizSize(iframe, width, height);
    if (!container) container = document.body;
    container.appendChild(iframe);
  }
  return iframe;
}

function setIframeVizSize (iframe, width, height) {
  iframe.width = width || window.innerWidth;
  iframe.height = height || window.innerHeight * 0.9;
 }

function displayDataOnCelOv (datasetInfoArray, compareDatasets) {
  let description = datasetInfoArray.map(x=>x.params.valueType).join(', ') + " levels on Metabolic Map";
  let iframe = generateIframeViz(description);
  const vizParams = new URLSearchParams();
  const popupTypeSelector = document.getElementById('popupType');
  vizParams.append("orgid", orgId());
  if (datasetInfoArray.length == 1) {
    vizParams.append("omics", "t");
    vizParams.append("datakey", datasetInfoArray[0].key);
    vizParams.append("popupLabelStyle", "columns");
    if (compareDatasets && compareDatasets.length) {
      vizParams.append("compare", compareDatasets.map(x=>x.key).join());
      if (popupTypeSelector.value == 'heat')
	popupTypeSelector.value = 'plot';
    }
  }
  else {
    let desc = `${datasetInfoArray[0].key}:Edge-Color,${datasetInfoArray[1].key}:Edge-Thickness`;
    vizParams.append("multiomics", desc);
  }
  vizParams.append("omicsPopups", popupTypeSelector.value);
  const url = `${ptoolsBaseUrl}/overviewsWeb/celOv.shtml?${vizParams}`;
  iframe.src = url;
}

function displayDataOnDashboard (datasetInfoArray) {
  let description = datasetInfoArray.map(x=>x.params.valueType).join(', ') + " levels on Omics Dashboard";
  let iframe = generateIframeViz(description);
  const vizParams = new URLSearchParams();
  if (datasetInfoArray.length == 1) {
    vizParams.append("dataset", datasetInfoArray[0].key);
  }
  else {
    vizParams.append('multiomics', 't');
    for (var i = 0; i< datasetInfoArray.length; i++) {
      vizParams.append(`dataset${i+1}`, datasetInfoArray[i].key);
    }
  }
  const url = `${ptoolsBaseUrl}/dashboard/dashboard.html?${vizParams}`;
  iframe.src = url;
}

function displayDataOnPathwayPage (datasetInfoArray, compareDatasets) {
  let description = datasetInfoArray[0].params.valueType + " levels on " + document.getElementById('pwyName').value;
  let iframe = generateIframeViz(description);
  const vizParams = new URLSearchParams();
  const popupTypeSelector = document.getElementById('popupType');
  vizParams.append("orgid", orgId());
  vizParams.append("id", document.getElementById('pwyID').value);
  vizParams.append("detail-level", 2);
  vizParams.append("omics", "T");
  vizParams.append("popupLabelStyle", "columns");
  vizParams.append("datakey", datasetInfoArray[0].key);
  if (compareDatasets && compareDatasets.length) {
    vizParams.append("compare", compareDatasets.map(x=>x.key).join());
    if (popupTypeSelector.value == 'heat')
      popupTypeSelector.value = 'plot';
  }
  vizParams.append("omicsPopups", popupTypeSelector.value);
  const url = `${ptoolsBaseUrl}/pathway?${vizParams}`;
  iframe.src = url;
}


function ptoolsGoBtnHandler () {
  const ptoolsDisplay = document.getElementById("ptoolsDisplay").value;
  const update = document.getElementById('addOrUpdate')?.value == 'update';
  if (update) updateExistingDisplaysBtnHandler();
  else if (ptoolsDisplay == "celov") celovBtnHandler();
  else if (ptoolsDisplay == "dashboard") dashboardBtnHandler();
  else if (ptoolsDisplay == "pwy") pwyBtnHandler();
  else if (ptoolsDisplay == "pwyWg") pwyBtnHandler(true);
  else if (ptoolsDisplay == "celovWg") celovBtnHandler(true);
}

async function celovBtnHandler (wgOnly, suppressPanelConfigP, vizDivConfig) {
  const valueTypes = allSelectedValueTypes();
  if (!valueTypes.length)
    alert("Please select a type of data to show.");
  else if (valueTypes.length > 2)
    alert("At most 2 types of data can be selected for this operation.");
  else {
    const simQueryParams = assembleSimQueryParams();
    const datasetInfoArray = await fetchAndRegisterAllSimData(valueTypes, simQueryParams);
    if (!datasetInfoArray.length) {
      alert("The requested data could not be loaded.");
      return;
    }
    else if (datasetInfoArray.length < valueTypes.length)
      alert("One or more of the requested datasets could not be loaded. Showing only the data that was successfully loaded.");
    const compareDatasets = valueTypes.length == 1 && await getCompareDatakeys(simQueryParams, valueTypes[0]);
    if (wgOnly) displayDataOnCelOvDiagram(datasetInfoArray, compareDatasets, suppressPanelConfigP, vizDivConfig);
    else displayDataOnCelOv(datasetInfoArray, compareDatasets, compareDatasets);
  }
}

async function dashboardBtnHandler () {
  const valueTypes = allSelectedValueTypes();
  if (!valueTypes.length)
    alert("Please select a type of data to show.");
  else if (valueTypes.length > 3)
    alert("At most 3 types of data can be selected for this operation.");
  /*
  else if (valueTypes.length == 1) {
    const datasetInfo = await fetchAndRegisterSimData(assembleSimQueryParams());
    if (datasetInfo.key) displayDataOnDashboard(datasetInfo);
    }
  */
  else {
    const datasetInfoArray = await fetchAndRegisterAllSimData(valueTypes, assembleSimQueryParams());
    if (!datasetInfoArray.length) {
      alert("The requested data could not be loaded.");
      return;
    }
    else if (datasetInfoArray.length < valueTypes.length)
      alert("One or more of the requested datasets could not be loaded. Showing only the data that was successfully loaded.");
    displayDataOnDashboard(datasetInfoArray);
  }
}

async function pwyBtnHandler (wgOnly, suppressPanelConfigP, vizDivConfig) {
  const valueTypes = allSelectedValueTypes();
  const pwyId = (vizDivConfig) ? vizDivConfig['pwyId'] : document.getElementById('pwyID').value
  if (!pwyId)
    alert ("You must specify a pathway to show.");
  else if (!valueTypes.length)
    alert("Please select a type of data to show.");
  else if (valueTypes.length > 1)
    alert("Only one type of data can be selected for this operation.");
  else {
    const simQueryParams = assembleSimQueryParams();
    const datasetInfoArray = await fetchAndRegisterAllSimData(valueTypes, simQueryParams);
    if (!datasetInfoArray.length) {
      alert("The requested data could not be loaded.");
      //return;
    }
    let compareDatasets = false
    if (datasetInfoArray.length) {
	compareDatasets = await getCompareDatakeys(simQueryParams, valueTypes[0]);
    }
    if (wgOnly)
      //displayDataOnPathwayDiagramInternal (pwyId, pwyName, datakey, compareKeys, suppressPanelConfigP, vizDivConfig)
      displayDataOnPathwayDiagram(datasetInfoArray, linkTarget, compareDatasets, suppressPanelConfigP, vizDivConfig);
    else displayDataOnPathwayPage(datasetInfoArray, compareDatasets);
  }
}

async function getCompareDatakeys (simQueryParams, valueType) {
  const compareIds = getCompareExptIds();
  const datakeys = [];
  if (compareIds) {
    const n_tp = getSim(simQueryParams.experiment_id).analyses.find(x=>x.database_id==simQueryParams.analysis_id).n_tps;
    for (let id of compareIds) {
      const compareSim = getSim(id);
      const compareAnalyses = await getAnalysesForSim(compareSim);
      const compareAnalysis = compareAnalyses.find(x=>x.n_tp == n_tp) || compareAnalyses[0];
      const params = { ...simQueryParams };
      params.experiment_id = id;
      params.analysis_id = compareAnalysis.database_id;
      params.valueType = valueType;
      const datakey = await fetchAndRegisterSimData(params);
      datakeys.push(datakey);
    }
  }
  return compareIds && datakeys;
}

function displayMassFractionSummary () {
  const description = "Mass Fraction Summary";
  let iframe = generateIframeViz(description, 600, 400);
  let searchParams = new URLSearchParams();
  searchParams.append('experiment_id', document.querySelector('input[name="experiment_id"]').value);
  //searchParams.append('filename', 'mass_fraction_summary.html');
  let url = `${ptoolsBaseUrl}/sms/fetchMassFractionSummary?${searchParams}`;
  iframe.src = url;
}

function allSelectedValueTypes () {
  return Array.from(document.querySelectorAll('input[name="valueType"]:checked'), x=>x.value);
}


function assembleSimQueryParams (analysisSelectionParent) {
  if (!analysisSelectionParent)
    analysisSelectionParent = document.getElementById('analysisSelectionCtrls');
  return {
    experiment_id: analysisSelectionParent.querySelector('.experimentId').value,
    analysis_id: parseInt(analysisSelectionParent.querySelector('.analysisId').value),
    valueType: document.querySelector('input[name="valueType"]:checked').value,
    orgid: orgId(),
  };
}

function getExperimentId (containerSelector) {
  return document.querySelector(containerSelector).querySelector('select.simSelector').value;
}

function getCompareExptIds () {
  if (document.getElementById('addCompare').checked) {
    const selected = Array.from(document.getElementById('compareSelector').options)
	  .filter(option => option.selected)
	  .map(option => option.value);
    if (selected && selected.length) return selected;
  }
}

function initControls () {
  document.getElementById('ptoolsGoBtn').addEventListener('click', ptoolsGoBtnHandler);
  smsACNameSelector(document.getElementById('pwyName'), document.getElementById('pwyNameContainer'), document.getElementById('pwyID'), "PATHWAY", true, true);
  document.getElementById('ptoolsDisplay').addEventListener('change', setPtoolsDisplayParams);
  document.getElementById('addCompare').addEventListener('change', onCompareChange);
  document.getElementById('valueTypeSelection').addEventListener('change', onValueTypeChange);
  setPtoolsDisplayParams();
  //kr:Jun-15-2026
  let allPanelVizContainer = document.getElementById("panelVisualizations"); // in panelConfigs.html
  if (allPanelVizContainer) {
      document.getElementById('createNewConfigBtn').addEventListener('click', createNewConfigBtnHandler);
      document.getElementById('panelConfigName').addEventListener('change', changedPanelConfigHandler);
      panelConfigsRetrieval()
  }
}

window.addEventListener('load', function() {
  let allPanelVizContainer = document.getElementById("panelVisualizations"); // in panelConfigs.html
  if (allPanelVizContainer) {
      populateSimulationSelector();
      populateOrgSelector();
      initControls();
  }
});

function setPtoolsDisplayParams () {
  let displayType = document.getElementById('ptoolsDisplay').value;
  if (displayType == 'pwy' || displayType == 'pwyWg' || displayType == 'celovWg') {
    document.getElementById('popupTypeBlock').style.display = "";
    document.querySelectorAll('input[name="valueType"]').forEach(elt => elt.type = "radio");
    document.getElementById('maxTypesStr').innerHTML = "one";
    document.getElementById('compareParams').style.display = "";
    if (displayType == 'celovWg') {
      document.getElementById('pwyAcBlock').style.display = "none";
      document.getElementById('popupNone').style.display = "none";
      if (document.getElementById('popupType').value == 'none')
	document.getElementById('popupType').selectedOption = 0;
      document.getElementById('celovCompareMsg').style.display = (document.getElementById('addCompare').checked) ? "" : "none";
    }
    else {
      document.getElementById('pwyAcBlock').style.display = "";
      document.getElementById('popupNone').style.display = "";
      document.getElementById('celovCompareMsg').style.display = "none";
    }
  }
  else {
    document.getElementById('pwyAcBlock').style.display = "none";
    document.getElementById('popupTypeBlock').style.display = "none";
    document.querySelectorAll('input[name="valueType"]').forEach(elt => elt.type = "checkbox");
    document.getElementById('maxTypesStr').innerHTML = "up to "+((displayType == 'celov') ? "two" : "three")+" types of data";
    document.getElementById('compareParams').style.display = (displayType == 'celov') ? '' : 'none';
    if (displayType == 'celov') {
      document.getElementById('popupTypeBlock').style.display = "";
      document.getElementById('popupNone').style.display = "none";
      if (document.getElementById('popupType').value == 'none')
	document.getElementById('popupType').selectedOption = 0;
      document.getElementById('celovCompareMsg').style.display = (document.getElementById('addCompare').checked) ? "" : "none";
    }
  }
}

/* Not used */
function initTimeRangeSliderElt (maxSecs, container) {
  if (maxSecs < 60*60*3) {
    document.getElementById('timeH').style.display = 'none';
    document.querySelector('input[name="timeUnit"][value="s"]').checked = true;
  }
  if (maxSecs < 60*3) {
    document.getElementById('timeM').style.display = 'none';
    document.getElementById('timeS').style.display = 'none';
    document.querySelector('input[name="timeUnit"][value="s"]').checked = true;
  }
  let unit = document.querySelector('input[name="timeUnit"]:checked').value;
  let multiplier = (unit == 'h') ? 3600 : (unit == 'm') ? 60 : 1;
  let minTextElt = container.querySelector('input.sliderMinText');
  let maxTextElt = container.querySelector('input.sliderMaxText');
  let sliderElt = container.querySelector('div.sliderWidget');
  function resetVals (newValues) {
    let maxVal = $j(sliderElt).slider("option", "max");
    sliderElt.style.maxWidth = (maxVal*20) + 'px';
    let values = newValues || [0, maxVal];
    $j(sliderElt).slider("option", "values", values);
    minTextElt.value = values[0];
    maxTextElt.value = values[1];
  }
  if ($j(sliderElt).slider("instance")) {
    $j(sliderElt).slider("option", "max", maxSecs / multiplier);
    resetVals();
    }
  else {
    const maxVal = maxSecs / multiplier;
    $j(sliderElt).slider({
      min: 0,
      max: maxVal,
      range: true,
      orientation: 'horizontal',
      slide: function (evt, ui) {
	if (ui.values[0] == ui.values[1]) {
	  evt.preventDefault();
	}
	else {
	  minTextElt.value = ui.values[0];
	  maxTextElt.value = ui.values[1];
	}
      }
    });
    minTextElt.addEventListener('change', function () {
      let maxVal = $j(sliderElt).slider("option", "max");
      let value = parseInt(minTextElt.value);
      value = Math.max(0, Math.min(value, maxVal - 1));
      minTextElt.value = value;
      maxTextElt.value = Math.max(value + 1, maxTextElt.value);
      $j(sliderElt).slider("option", "values", [minTextElt.value, maxTextElt.value]);
    });
    maxTextElt.addEventListener('change', function () {
      let maxVal = $j(sliderElt).slider("option", "max");
      let value = parseInt(maxTextElt.value);
      value = Math.max(1, Math.min(value, maxVal));
      maxTextElt.value = value
      minTextElt.value = Math.min(value - 1, minTextElt.value);
      $j(sliderElt).slider("option", "values", [minTextElt.value, maxTextElt.value]);
    });
    document.querySelectorAll('input[name="timeUnit"]').forEach(elt => {
      elt.addEventListener('change', (evt) => {
	const oldMultiplier = multiplier;
	const oldMax = $j(sliderElt).slider("option", "max");
	const oldVals = $j(sliderElt).slider("option", "values");
	multiplier = (elt.value == 'h') ? 3600 : (elt.value == 'm') ? 60 : 1;
	$j(sliderElt).slider("option", "max",  oldMax*oldMultiplier/multiplier);
	resetVals([parseInt(oldVals[0]*oldMultiplier/multiplier), parseInt(oldVals[1]*oldMultiplier/multiplier)]);
      });
    });
  }
  resetVals();
}

function collapseIframeViz (vizDiv, description) {
  vizDiv.classList.add('thumbnail');
}

function expandIframeViz (vizDiv) {
  vizDiv.classList.remove('thumbnail');
}

// I've removed the org and orgs args from the regular ptools ACNameSelector fn.
function smsACNameSelector(nameField, container, idField, searchType, excludeClasses, autofillExactMatch, addlQueryFields) {
  let nameFieldObj = $j(nameField);
  let queryData = { type: searchType,
		    max: 2000,
		  };
  if (searchType == 'COMPOUND' || searchType == 'GENE')
    queryData.markclasses = 't';
  if (excludeClasses) queryData.noclasses = 't';
  if (addlQueryFields) $j.extend(queryData, addlQueryFields);
  nameFieldObj.prop('acQueryArgs', queryData);
  if (idField) nameFieldObj.keydown(()=> idField.value = "");
  let exactMatch;
  nameFieldObj.autocomplete({
    source: function (request, response) {
      queryData.object = request.term;
      exactMatch = null;
      if (idField) idField.value = "";
      $j.ajax( {
	url: `/${orgId()}/ajax-frame-search`,
	data: queryData,
	dataType: "json",
	success: function (responseData) {
	  response(responseData.Results);
	}
      });
    },
    select: function (evt, ui) {
      if (idField) idField.value = ui.item.id;
      nameField.value = ui.item.label;
    },
    change: function (evt, ui) {
      if (autofillExactMatch && exactMatch && idField && !idField.value) {
	idField.value = exactMatch.id;
	nameField.value = exactMatch.label;
      }
    },
    close: function (evt, ui) {
      if (autofillExactMatch && exactMatch && idField && !idField.value) {
	idField.value = exactMatch.id;
	nameField.value = exactMatch.label;
      }
    }
  })
    .autocomplete("instance")._renderItem = function (ul, item) {
      if (queryData.object.toUpperCase() == item.label.toUpperCase())
	exactMatch = item;
      return $j("<li>").html(item.qName).appendTo(ul);
    };
  return nameFieldObj;
};

function populateOrgSelector () {
  return fetch(`${ptoolsBaseUrl}/get-organisms-json`)
    .then(response => response.json())
    .then(data => {
      const selectElt = document.getElementById('orgSelect');
      for (let org of data) {
	let option = new Option(org.label, org.id);
	selectElt.add(option);
      }
      return true;
    });
}

/* Populate both the initial simulation selector, and the selector used for
   selecting comparison simulations with all available simulations. This
   includes setting up the tag filters. This is only called once, on inital
   page load.
 */
function populateSimulationSelector () {
  const selectContainer = document.getElementById('exptId');
  const selectElt = selectContainer.querySelector('select.simSelector');
  const compareSelectContainer = document.getElementById('compareParams');
  populateSimulationSelectOptions([selectContainer, compareSelectContainer])
    .then(()=>{
      selectElt.addEventListener('change', function () {
	if (!selectElt.value) noSimSelected();
	//else if (selectElt.value == 'new') newSimSelected();
	else simSelected(selectElt.value);
      });
    });
}

/* Fetch the complete list of simulations via the SMS API. Given a list of 
   elements that each include a simulation select element (select.simSelector),
   populate each with the retrieved list of simulations. For each
   selectorContainer element that also includes a .tagSelector element, 
   populate it with the list of simulation tags, and implement the tag-filtering
   functionality.
 */
function populateSimulationSelectOptions (selectorContainers) {
  const simSelector = (elt) => elt.querySelector('select.simSelector');
  const tagSelector = (elt) => elt.querySelector('select.tagSelector');
  selectorContainers.forEach(elt=>simSelector(elt).disabled = true);
  let simsResponse;
  function updateSelector (selectorContainer) {
    const selectElt = simSelector(selectorContainer);
    const tag = tagSelector(selectorContainer).value;
    selectElt.replaceChildren();
    allSimulations.forEach(function (sim) {
      const exptId = sim.config?.experiment_id;
      let name = exptId;
      if (sim.name || sim.config.description)
	name += ` (${sim.name || sim.config.description})`;
      if (exptId && (!tag || sim.tags.includes(tag))) {
	const option = new Option(name, exptId);
	selectElt.add(option, 0);
      }
    });
    if (!selectElt.multiple) selectElt.add(new Option("--Select a Simulation--", ""), 0);
    selectElt.value = "";
    selectElt.disabled = false;
  }
  return fetch(`${simBaseUrl}simulations`)
    .then(response => {
      simsResponse = response;
      return response.json()
    })
    .then(data =>{
      if (simsResponse.ok) {
	allSimulations = data;
	allSimulations.sort((a,b)=>a.database_id - b.database_id);
	const allTags = new Set();
	allSimulations.forEach(sim=>sim.tags?.forEach(tag=>allTags.add(tag)));
	const tagArray = [...allTags].sort((a,b)=>a.localeCompare(b));
	selectorContainers.forEach(elt=>{
	  const tagElt = tagSelector(elt);
	  tagElt.replaceChildren();
	  tagArray.forEach(tag=>{
	    const option = new Option(tag, tag);
	    tagElt.add(option);
	  });
	  tagElt.add(new Option("-- No Filter --", ""), 0);
	  tagElt.value = "";
	  tagElt.addEventListener('change', ()=>updateSelector(elt));
	});
	selectorContainers.forEach(elt=>updateSelector(elt));
	return true;
      }
      else throw new Error(`Error ${simsResponse.status} fetching list of simulations: ${data.detail}`);
    });
}


function noSimSelected () {
  document.getElementById('simParams').style.display = 'none';
  document.getElementById('newSimParams').style.display = 'none';
  document.getElementById('controls1').style.display = 'none';
}

/*
function newSimSelected () {
  document.getElementById('simParams').style.display = 'none';
  document.getElementById('newSimParams').style.display = '';
  document.getElementById('controls1').style.display = 'none';
  }
*/

function simSelected (exptId) {
  const elt = document.getElementById('simParams');
  const compareSelectElt = document.getElementById('compareSelector');
  const sim = getSim(exptId);
  if (sim) {
    let html = ""; //`Experiment Name: ${simData.name || exptId}`;
    const generations = sim.config.generations;
    const seeds = sim.config.n_init_sims;
    if (generations) {
      html = `${html}<div>Generations: ${generations}</div>`;
    }
    if (seeds) {
      html = `${html}<div>Lineage Seeds: ${seeds}</div>`;
    }
    elt.innerHTML = html;
    getAnalysesForSim(sim)
      .then(analyses=>{
	populateAnalysisSelector(analyses);
	document.getElementById('simParams').style.display = '';
	document.getElementById('controls1').style.display = '';
      });
  }
  for (const option of compareSelectElt.options) {
    if (option.value == exptId) option.selected = false;
    option.disabled = (option.value == exptId);
  }
  // If simConfig includes PGDB id, set orgSelect value accordingly, otherwise
  // set to ECOLI. (this is just a placeholder -- I don't yet know how/if this
  // parameter will be represented in simConfig).
  const orgid = sim.config.pgdb || defaultOrgid;
  const orgselect = document.getElementById('orgSelect');
  if (orgselect.querySelector(`option[value="${orgid}"]`))
    orgselect.value = orgid;
}

// Return a promise that resolves to an array of analyses
function getAnalysesForSim (sim) {
  if (sim.analyses) return Promise.resolve(sim.analyses);
  return fetch(`${simBaseUrl}analyses?experiment_id=${sim.experiment_id}`)
    .then(response=>response.json())
    .then(analyses=>{
      sim.analyses = analyses;
      return analyses;
    });
}

/* For now, we only have one analysis id per simulation, but presumably that
   will change...
 */
function populateAnalysisSelector (analyses, parent) {
  function analysisDesc (analysis) {
    let desc = `Number of intervals: ${analysis.n_tp}`;
    /* We may add other properties here in the future */
    return desc;
  }
  if (!parent) parent = document.getElementById('analysisSelectionCtrls');
  parent.replaceChildren();
  if (!analyses?.length) {
    parent.innerHTML = "There is no analysis data available for this simulation."
  }
  else if (analyses.length == 1) {
    const input = document.createElement('input');
    input.classList.add('analysisId');
    input.type='hidden';
    input.value = analyses[0].database_id;
    const text = document.createElement('span');
    text.innerHTML = `Analysis Configuration: ${analysisDesc(analyses[0])}`;
    parent.appendChild(text);
    parent.appendChild(input);
  }
  else {
    const label = createElement('label');
    const prompt = createElement('span');
    prompt.classList.add('paramLabel');
    prompt.innerHTML = "Select Analysis Configuration"
    const select = createElement('select');
    select.classList.add('analysisId');
    analyses.forEach(analysis=>{
      const option = new Option(analysisDesc(analysis), analysis.database_id);
      select.add(option, 0);
    });
    label.appendChild(prompt);
    label.appendChild(select);
    parent.appendChild(label);
  }
  const exptId = analyses[0]?.experiment_id;
  if (exptId) {
    const exptIdInput = document.createElement('input');
    exptIdInput.classList.add('experimentId');
    exptIdInput.type='hidden';
    exptIdInput.value = exptId;
    parent.appendChild(exptIdInput);
  }
}

/* This is obsolete.
function populateAnalysisSelectionCtrls (simConfig, parent) {
  if (!parent) parent = document.getElementById('analysisSelectionCtrls');
  const generations = simConfig.generations;
  const seeds = simConfig.n_init_sims;
  let genSelect = parent.querySelector('.generationSelector');
  let seedSelect = parent.querySelector('.seedSelector');
  while (seedSelect.options.length > 1) seedSelect.options[1].remove();
  while (genSelect.options.length > 1) genSelect.options[1].remove();
  if (generations && generations > 1) {
    for (let i=0; i<generations; i++)
      genSelect.add(new Option(i, i));
    genSelect.selectedIndex = 0;
  }
  if (seeds && seeds > 1) {
    for (let i=0; i<seeds; i++)
      seedSelect.add(new Option(i, i));
    seedSelect.selectedIndex = 0;
  }
  setAnalysisSelectionVisibility(parent);
  seedSelect.addEventListener('change', ()=>setAnalysisSelectionVisibility(parent));
}

function setAnalysisSelectionVisibility (parent) {
  if (!parent) parent = document.getElementById('analysisSelectionCtrls');
  let seedCtrls = parent.querySelector('.multiseed');
  let seedSelect = parent.querySelector('.seedSelector');
  let genCtrls = parent.querySelector('.multigen');
  let genSelect = parent.querySelector('.generationSelector');
  if (seedSelect.options.length > 1) {
    parent.style.display = '';
    seedCtrls.style.display = '';
    if (seedSelect.selectedIndex > 0 && genSelect.options.length > 1) {
      genCtrls.style.display = '';
    }
    else {
      genCtrls.style.display = 'none';
      genSelect.selectedIndex = 0;
    }
  }
  else {
    seedCtrls.style.display = 'none';
    if (genSelect.options.length > 1) {
      genCtrls.style.display = '';
      parent.style.display = '';
    }
    else {
      genCtrls.style.display = 'none';
      parent.style.display = 'none';
    }
  }
}

function getSimConfig (exptId) {
  const sim = allSimulations.find(x=>x.config?.experiment_id == exptId);
  if (sim) return sim.config;
}
*/

function getSim (exptId) {
  return allSimulations.find(x=>x.config?.experiment_id == exptId);
}

function onCompareChange (evt) {
  const checkedP = evt.target.checked;
  document.querySelectorAll('#compareParams .compare').forEach(x=>x.style.display = (checkedP) ? '' : 'none');
  document.getElementById('celovCompareMsg').style.display = (checkedP && document.getElementById('ptoolsDisplay').value == 'celov') ? '' : 'none';
  if (allSelectedValueTypes().length > 1) {
    alert("Comparisons are not available when multiple data types are selected.");
    return false;
  }
}

function onValueTypeChange (evt) {
  document.getElementById('compareParams').style.display = (allSelectedValueTypes().length > 1 || document.getElementById('ptoolsDisplay').value == 'dashboard') ? 'none' : '';
}

let workingMsgCounter = 0;
let workingMsgStack = [];

function showWorkingMsg (msg) {
  let counter = ++workingMsgCounter;
  workingMsgStack.push([counter,msg]);
  document.getElementById('workingMsgContents').innerHTML = msg || "Working...";
  document.getElementById('workingMsg').style.display = 'block';
  console.log('Show '+counter+', Stack: '+workingMsgStack);
  return counter;
}

function hideWorkingMsg (counter) {
  if (counter) 
    workingMsgStack = workingMsgStack.filter(x=>x[0] != counter);
  else workingMsgStack = [];
  console.log('Hide '+counter+', Stack: '+workingMsgStack);
  if (workingMsgStack.length)
    document.getElementById('workingMsgContents').innerHTML = workingMsgStack[workingMsgStack.length - 1][1] || "Working...";
  else document.getElementById('workingMsg').style.display = 'none';
}

function currentWorkingMsg () {
  if (document.getElementById('workingMsg').style.display == 'block')
    return document.getElementById('workingMsgContents').innerHTML;
}

function withWorkingMsg(bodyFn) {
  let msgCounter = showWorkingMsg();
  // Add a short timeout to give the working msg time to appear before we start working.
  setTimeout(function () {
    bodyFn();
    hideWorkingMsg(msgCounter);
  }, 20);
}

function displayDataOnPathwayDiagram (datasetInfoArray, linkTarget, compareDatasets, suppressPanelConfigP, vizDivConfig) {
  //const pwyId = document.getElementById('pwyID').value;
  const pwyId = (vizDivConfig) ? vizDivConfig['pwyId'] : document.getElementById('pwyID').value
  //const pwyName = document.getElementById('pwyName').value;
  const pwyName = (vizDivConfig) ? vizDivConfig['title'] : document.getElementById('pwyName').value
  const datakey = (datasetInfoArray[0]) ? datasetInfoArray[0].key : false
  const orgid = orgId();
  const href = `/pathway?orgid=${orgid}&id=${pwyId}`;
  const msgCounter = showWorkingMsg("Fetching pathway diagram...");
  const params = {"detail-level": 2};
  const compareKeys = compareDatasets && compareDatasets.map(x=>x.key);
  return pwyToWgsnap(orgid, pwyId, datakey, params, compareKeys)
    .then(wgsnap=>{
      if (linkTarget) wgsnap.wg.linkTarget = linkTarget;
	//const div = generateWgDiv(`${orgid}_${pwyId}`, pwyName, href, pwyId);
	const div = generateWgDiv(`${orgid}_${pwyId}`, pwyName, href, pwyId, suppressPanelConfigP, vizDivConfig);
      //div.dataset.wgsnap = wgsnap;
      div.dataset.orgid = orgid;
      div.dataset.displaytype = 'pwyWg';
      div.dataset.frameid = pwyId;
      div.dataset.params = JSON.stringify(params);
      return WG.SnapshotToDiv(wgsnap, div);
    })
    .finally(()=>{
	// Grab and store the existing configs of wgDivs displayed
	let allPanelVizContainer = document.getElementById("panelVisualizations"); // in panelConfigs.html
	if (allPanelVizContainer) recordAllVizDivConfigs()
	
	hideWorkingMsg(msgCounter)
    });
}

/// These arguments can be false : datakey, compareKeys
/// (This was partially split out from displayDataOnPathwayDiagram() )
///
function displayDataOnPathwayDiagramInternal (pwyId, pwyName, datakey, compareKeys, suppressPanelConfigP, vizDivConfig) {
    //console.log('pwyName: ' + pwyName + '  pwyId: ' + pwyId)
    const orgid = orgId();
    const href = `/pathway?orgid=${orgid}&id=${pwyId}`;
    const params = {"detail-level": 2};
    return pwyToWgsnap(orgid, pwyId, datakey, params, compareKeys)
	.then(wgsnap=>{
	    if (linkTarget) wgsnap.wg.linkTarget = linkTarget;
	    const div = generateWgDiv(`${orgid}_${pwyId}`, pwyName, href, pwyId, suppressPanelConfigP, vizDivConfig);
	    //div.dataset.wgsnap = wgsnap;
	    div.dataset.orgid = orgid;
	    div.dataset.displaytype = 'pwyWg';
	    div.dataset.frameid = pwyId;
	    div.dataset.params = JSON.stringify(params);
	    WG.SnapshotToDiv(wgsnap, div)
	    //moveVizDivToCoords (div, vizDivConfig)
	})
}

///kr:Jul-8-2026 An example of restoring one panel:
/// displayDataOnPathwayDiagramInternal("PWY-5436", "Lthre hack", false, false, true, panelConfigs["trehal 2c"]["trehalose biosynthesis I (Pathways)"])

function displayDataOnCelOvDiagram (datasetInfoArray, compareDatasets, suppressPanelConfigP, vizDivConfig) {
  const datakey = datasetInfoArray[0].key;
  const compareKeys = compareDatasets && compareDatasets.map(x=>x.key);
  const orgid = orgId();

  let allPanelVizContainer = document.getElementById("panelVisualizations"); // in panelConfigs.html
  if (allPanelVizContainer) {
      /* Setting this to true will prevent the Omics data getting removed when calling OP.Close() */
      KR_HAX = {}
      KR_HAX.suppressOnClose = true
      console.log('KR_HAX.suppressOnClose: ' + KR_HAX.suppressOnClose)
      //OP.suppressOnClose = true // This simply was overwritten by another later load of the OP code !
      //console.log('OP.suppressOnClose 1: ' + OP.suppressOnClose)
  }
  const msgCounter = showWorkingMsg("Fetching metabolic map...");
  return celovToWgsnap(orgid, datakey, compareKeys)
    .then(wgsnap=>{
      const div = generateWgDiv('mapCel', "Metabolic Map", `/overviewsWeb/celOv.shtml?orgid=${orgid}`, false, suppressPanelConfigP, vizDivConfig);
      //div.dataset.wgsnap = wgsnap;
      div.dataset.orgid = orgid;
      div.dataset.displaytype = 'celovWg';
      console.log('displayDataOnCelOvDiagram  div.dataset.displayType: ' + div.dataset.displaytype)
	//return  //kr:Jul-14-2026 not really needed here
	WG.SnapshotToDiv(wgsnap, div)
	//moveVizDivToCoords (div, vizDivConfig)
    })
	.finally(()=>{
	    //moveVizDivToCoords (div, vizDivConfig)
	    /* Setting this to true will prevent the Omics data getting removed when calling OP.Close() */
	    //OP.suppressOnClose = true // This simply was overwritten by another later load of the OP code !
	    //console.log('OP.suppressOnClose 2: ' + OP.suppressOnClose)

	    hideWorkingMsg(msgCounter)
	});
}

/*
const wgDivResizeObserver = new ResizeObserver(entries=>{
  for (const entry of entries) {
    const div = entry.target;
    const canvas = div.querySelector('canvas');
    if (canvas) {
      const width = div.clientWidth;
      const height = div.clientHeight;
      canvas.style.width = width+'px';
      canvas.style.height = height+'px';
      WG.OnResize();
    }
  }
});
*/

function generateWgDiv (divId, title, href, pwyId, suppressPanelConfigP, vizDivConfig) {
  let initialWidth  = (vizDivConfig) ? vizDivConfig['width'] : Math.min(window.innerWidth - 100, 800);
  let initialHeight = (vizDivConfig) ? vizDivConfig['height'] : Math.min(window.innerHeight - 100, 800);
  let allPanelVizContainer = document.getElementById("panelVisualizations"); // in panelConfigs.html
  let allVizContainer = (allPanelVizContainer) ? allPanelVizContainer : document.getElementById("visualizations");
  let vizDiv = document.createElement('div');
  vizDiv.classList.add("resizableDraggable");
  vizDiv.classList.add("wgContainer");
  console.log('generateWgDiv divId : ' + divId + ' href: ' + href)

  if (allPanelVizContainer) {
      //vizDiv.style.position = 'fixed' // at the .style level, fixed did have an effect !
      vizDiv.style.position = 'absolute'
      vizDiv.style.width = initialWidth+'px';
      vizDiv.style.height = initialHeight+'px';
      if (vizDivConfig) {
	  console.log('generateWgDiv vizDiv.style.left (x): ' + vizDiv.style.left)
	  vizDiv.style.left = vizDivConfig['x']+'px';
	  vizDiv.style.top  = vizDivConfig['y']+'px';
      }
  }
  allVizContainer.append(vizDiv);

  let titleControlsParentDiv = document.createElement('div');
  titleControlsParentDiv.classList.add("titleControlsParentDiv");
  let titleDiv = document.createElement('div');
  titleDiv.classList.add('titleBar');
  titleDiv.innerHTML = (href) ? `<a href='${href}' target=${linkTarget}>${title}</a>` : title;
  //titleDiv.style.position = 'absolute'
  //vizDiv.append(titleDiv);
  //vizControls.append(titleDiv);
  //vizDiv.append(titleDiv)
  vizDiv.append(titleControlsParentDiv);
  titleControlsParentDiv.append(titleDiv)
    
  // Add a Close X icons on the right side of the title bar
  let vizControls = document.createElement('div');
  vizControls.classList.add("vizControls");
  //vizDiv.append(vizControls)
  //titleDiv.append(vizControls)
  titleControlsParentDiv.append(vizControls)

  titleDiv.style.width = '100%'
  //titleDiv.style.position = 'relative'
  //titleDiv.style.display = 'inline-block'  // cuts it too short !!!
  let overlay = document.createElement('div');
  overlay.classList.add("thumbnailOverlay");
  // Tear the overlay out of normal document flow
  overlay.style.position = 'absolute'  // placed the X underneath...
  //overlay.style.right = '8px' // also placed the X underneath...
  overlay.style.right = '100%' // also placed the X underneath...
  //vizControls.style.right = '100%' // also placed the X underneath...
  //vizControls.append(overlay); // also placed the X underneath...
  //titleDiv.append(overlay); // also placed the X underneath...
  titleControlsParentDiv.append(overlay); // also placed the X underneath...
  function addControlIcon (faClass, handler, ttip) {
    let icon = document.createElement('i');
    icon.className = faClass;
    if (ttip) icon.title = ttip;
    vizControls.append(icon)
    //overlay.append(icon)
    //titleDiv.append(icon); // could this put the icon on the same line as the title ?
    icon.addEventListener('click', handler);
  }
  addControlIcon("fa fa-close", ()=>{
      if (confirm('Really delete this diagram ?') ) {
	  deleteDiagramInCurrentConfig(title)
	  vizDiv.remove();
      }
  },
		 "Delete");

  let wgDiv = document.createElement('div');
  wgDiv.id = divId;
  wgDiv.style.width = initialWidth+'px';
  //console.log('wgDiv.style.width: ' + wgDiv.style.width)
  wgDiv.style.height = initialHeight+'px';
  /*
  if (allPanelVizContainer) {
      wgDiv.position = 'absolute' // not sure whether that ever did anything...
      wgDiv.style.left = vizDivConfig['x']+'px';
      wgDiv.style.top  = vizDivConfig['y']+'px';
      }
  */
  vizDiv.append(wgDiv);
  if ( suppressPanelConfigP == false ) {
      //redundant: populatePanelConfig(wgDiv, divId, pwyId, title, href)
      // Grab and store the existing configs of wgDivs displayed, if any
      recordAllVizDivConfigs()
  }
  /*
  const dragFn = function (evt, ui) {
    WG.OnResize();
    const div = ui.helper[0];
    const canvas = div.querySelector('canvas');
    if (canvas) {
      const wg = canvas.parentElement.wg;
      if (wg)
	WG.Draw(wg);
	}
  }
  */
  $j(vizDiv).draggable({ handle: titleDiv,
			 cancel: "a,button,input,select,option",
			 drag: WG.OnResize,
			 stop: function(event, ui) {
			     // 1. Run the prior existing resize/position updater function
			     WG.OnResize(event, ui); 
			     // 2. Capture absolute coordinates relative to the document
			     vizDivConfigStoreDragCoords (titleDiv, ui)
			 },
			 //stop: WG.OnResize,
			 stack: ".resizableDraggable",
			 distance: 0
		       });
  $j(vizDiv).resizable({ resize: (evt, ui)=>{
    const div = ui.element[0];
    const titlebar = div.querySelector('div.titleBar');
    const titlebarHt = titlebar?.clientHeight || 0;
    const canvas = div.querySelector('canvas');
    if (canvas) {
      const width = ui.size.width;
      const height = ui.size.height - titlebarHt;
      canvas.style.width = width+'px';
      canvas.style.height = height+'px';
      canvas.parentElement.style.width = '';
      canvas.parentElement.style.height = '';
      WG.OnResize();
	// 2. Capture absolute coordinates relative to the document
	vizDivConfigStoreResizeCoords (titleDiv, ui)
    }
  }
		       });
  return wgDiv;
}

function updateWgdivsOmics (datakey, compareDatakeys) {
  document.querySelectorAll('div.wgContainer').forEach(elt=>{
    const wgDiv = elt.querySelector('canvas').parentNode;
    const oldWg = wgDiv.wg;
    const linkTarget = oldWg.linkTarget;
    const displayType = wgDiv.dataset.displaytype;
    if (displayType == 'pwyWg') {
      const pwyId = wgDiv.dataset.frameid;
      const params = JSON.parse(wgDiv.dataset.params);
      return pwyToWgsnap(orgId(), pwyId, datakey, params)
	.then(wgsnap=>{
	  wgDiv.id = wgDiv.dataset.originalId;
	  if (linkTarget) wgsnap.wg.linkTarget = linkTarget;
	  return WG.SnapshotToDiv(wgsnap, wgDiv)
	});
    }
    else if (displayType == 'celovWg') {
      return celovToWgsnap(orgId(), datakey, compareDatakeys)
	.then(wgsnap=>{
	  // handle linkTarget here.
	  return WG.SnapshotToDiv(wgsnap, wgDiv);
	});
    }
  });
}

async function updateExistingDisplaysBtnHandler () {
  const valueTypes = allSelectedValueTypes();
  if (!valueTypes.length)
    alert("Please select a type of data to show.");
  else if (valueTypes.length > 1)
    alert("Only one type of data can be selected for this operation.");
  else {
    const simQueryParams = assembleSimQueryParams();
    const datasetInfoArray = await fetchAndRegisterAllSimData(valueTypes, simQueryParams);
    if (!datasetInfoArray.length) {
      alert("The requested data could not be loaded.");
      return;
    }
    //const compareDatasets = await getCompareDatakeys(simQueryParams, valueTypes[0]);
    updateWgdivsOmics(datasetInfoArray[0].key);
  }
}

async function silentUpdateExistingDisplaysBtnHandler () {
  const valueTypes = allSelectedValueTypes();
  /*
  if (!valueTypes.length)
    alert("Please select a type of data to show.");
  else if (valueTypes.length > 1)
    alert("Only one type of data can be selected for this operation.");
  else {
  */
    const simQueryParams = assembleSimQueryParams();
    const datasetInfoArray = await fetchAndRegisterAllSimData(valueTypes, simQueryParams);
    if (datasetInfoArray.length) {
      updateWgdivsOmics(datasetInfoArray[0].key);
      //alert("The requested data could not be loaded.");
      //return;
    }
    //const compareDatasets = await getCompareDatakeys(simQueryParams, valueTypes[0]);
  //}
}

/*************** Rank-Based Comparison Page *******************/

let allDatasets = {};
let cachedComparisons;

let rankComparison = {
  rankData: null,
  view: null, // Vega view
  datakey: null,
  correlation: null,
  stddev: null,
  table: null
}

function initRankCompare () {
  const msgCounter = showWorkingMsg("Initializing...");
  sourceDisableAll();
  let initPromises = [];
  initPromises.push(populateOrgSelector()
    .then(()=>{
      let promises = [];
      allOrganisms.forEach(org=>{
	promises.push(allAvailableOmicsDatasets(org.id, true)
		      .then(datasets=>{
			allDatasets[org.id] = datasets;
			if (document.getElementById('orgSelect').value == org.id)
			  populateDatakeySelectors(datasets || []);
			return datasets;
		      }));
      });
      return Promise.allSettled(promises);
    }));
  initPromises.push(populateSimulationSelectOptions(document.querySelectorAll('div.sourceDiv[id^="sim"]')));
  Array.from(document.getElementsByClassName('fileChoose')).forEach(x=>x.append(fileData.content.cloneNode(true)));
  initPrevComparisonSelector();
  Promise.allSettled(initPromises)
    .then(()=>{
      document.getElementById('valueType').addEventListener('change', ()=>populateDatakeySelectors()); 
      document.getElementById('orgSelect').addEventListener('change', ()=>populateDatakeySelectors());
      document.getElementById('retrieveDataButton').disabled = false;
      document.getElementById('comparisonId').disabled = false;
      enableSource(document.querySelector('input[name="source_1"]:checked'), 1);
      enableSource(document.querySelector('input[name="source_2"]:checked'), 2);
      hideWorkingMsg(msgCounter);
    });
  $j('#topNPanel').dialog({
    title: "Compare lists of N most abundant entities",
    autoOpen: false,
    width: 800,
    height: 1000,
    open: updateTopNTables
  });
  document.getElementById('topNCutoff').addEventListener('change', updateTopNTables);
}

function sourceDisableAll (parentId) {
  const parentElt = (parentId) ? document.getElementById(parentId) : document;
  parentElt.querySelectorAll('.sourceDiv')
    .forEach(elt=>elt.style.display='none');
}	

function enableSource (elt, index) {
  const baseName = elt.value;
  const parentId = index && 'dataset'+index;
  const enableId = (index) ? `${baseName}_${index}` : baseName;
  sourceDisableAll(parentId);
  document.getElementById(enableId).style.display = '';
}

function omicsSrcChanged(selector) {
  const parentElt = selector.closest('.sourceDiv');
  if (!parentElt) return;
  const datakeySelectedElts = parentElt.querySelectorAll('.datakeySelected');
  const noDatakeySelectedElts = parentElt.querySelectorAll('.noDatakeySelected');
  if (selector.value) {
    datakeySelectedElts.forEach(elt=>elt.style.display='');
    noDatakeySelectedElts.forEach(elt=>elt.style.display='none');
  }
  else {
    datakeySelectedElts.forEach(elt=>elt.style.display='none');
    noDatakeySelectedElts.forEach(elt=>elt.style.display='');
  }
}

function populateDatakeySelectors (datasets) {
  if (!datasets) datasets = allDatasets[document.getElementById('orgSelect').value] || [];
  const objectType = document.getElementById('valueType').value;
  if (datasets && datasets.length) {
    const selectors = document.querySelectorAll('select.keySelect');
    selectors.forEach(selector => {
      const keepOption = selector.querySelector('option[value=""]');
      selector.options.length = 0;
      for (let i=0; i<datasets.length; i++) {
	const ds = datasets[i];
	if (ds.expressiontype == 'absolute' && ds.objectType == objectType) {
	  const option = new Option(ds.title, ds.key);
	  selector.add(option);
	}
      }
      if (keepOption) selector.add(keepOption);
      selector.selectedIndex = 0;
      omicsSrcChanged(selector);
    });
  }
}

/* To do: add checks to ensure that fields are properly populated */
function getDatakeyForFieldset (i) {			       
  const source = document.querySelector(`[name='source_${i}']:checked`).value;
  const orgid = document.getElementById('orgSelect').value;
  const valueType = document.getElementById('valueType').value;
  if (source == 'exp') {			       
    const datakeySelector = document.getElementById(`datakeySelector_${i}`);
    if (datakeySelector.value)
      return Promise.resolve(datakeySelector.value);
    else {
      const formData = new FormData();
      const file = document.querySelector(`#fileSection${i} input[name="datafile"]`).files[0];
      const column = document.querySelector(`#fileSection${i} input[name="column1"]`).value;
      const title = document.querySelector(`#fileSection${i} input[name="title"]`).value;
      formData.append('datafile', file, file.name);
      formData.append('column1', column);
      formData.append('expressiontype', 'absolute');
      formData.append('class', valueType);
      formData.append('orgid', orgid);
      if (title) formData.append('title', title);
      const msgCounter = showWorkingMsg("Uploading Dataset "+i+"...");
      return fetch(`${ptoolsBaseUrl}/register-omics-dataset`, {
	method:'POST',
	body: formData
      })
      .then(response=>response.json())
      .then(result=>{
        if (result.success) {
	  saveOmics(formData, result);
	  addNewlyRegisteredDatasetToSelectors(result, i);
          return result.key;
        }
        else throw new Error(`Error uploading Dataset ${i}: ${status.error}`);
      })
	.finally(()=>hideWorkingMsg(msgCounter));
    }
  }
  else if (source == 'sim') {
    const exptId = document.getElementById(`simSelector${i}`).value;
    const sim = getSim(exptId);
    const simParams = {
      experiment_id: exptId,
      orgid: orgid,
      nIntervals: 10
    };
    if (valueType == 'protein') simParams.valueType = 'ptools_proteins';
    else if (valueType == 'gene') simParams.valueType = 'ptools_rna';
    else if (valueType == 'reaction') simParams.valueType = 'ptools_rxns';
    return getAnalysesForSim(sim)
      .then(analyses=>{
	simParams.analysis_id = analyses[0].database_id;
	return fetchAndRegisterSimData(simParams);
      })
      .then(result=>{
	addNewlyRegisteredDatasetToSelectors(result, i);
	return result.key;
      });
  }
  else throw new Error(`Dataset ${i} not specified.`);
}

function addNewlyRegisteredDatasetToSelectors (datasetInfo, i) {
  // Relevant info from uploaded file will be directly under datasetInfo, but
  // info from imported sim data will be in datasetInfo.params! This is a hack!
  const title = datasetInfo.title || datasetInfo.params?.title;
  const orgid = datasetInfo.orgid || datasetInfo.params?.orgid;
  document.querySelectorAll('select.keySelect').forEach(s=>{
    const option = new Option(title, datasetInfo.key)
    s.add(option, 0);
  });
  allDatasets[orgid].unshift(datasetInfo);
  document.querySelector(`[name='source_${i}'][value='exp']`).checked = true;
  const datakeySelector = document.getElementById(`datakeySelector_${i}`);
  datakeySelector.selectedIndex = 0;
  omicsSrcChanged(datakeySelector);
}

function retrieveCompareData (col1, col2) {
  document.getElementById('rankComparisonResults').style.display='none';
  clearSearch();
  let dataset1, dataset2
  const msgCounter = showWorkingMsg("Retrieving data...");
  return getDatakeyForFieldset(1)
    .then(key=>{
      return validateOmics(key)
	.then(()=>fetch(`${ptoolsBaseUrl}/fetchRegisteredDataset?key=${key}&attrs=gene,product,heteromultimer,pathway`));
    })
    .then(response=>response.json())
    .then(ds1=>dataset1=ds1)
    .then(()=>getDatakeyForFieldset(2))
    .then(key=>{
      return validateOmics(key)
	.then(()=>fetch(`${ptoolsBaseUrl}/fetchRegisteredDataset?key=${key}`));
    })
    .then(response=>response.json())
    .then(ds2=>dataset2=ds2)
    .then(()=>{
      if (dataset1.colNums.length==1 && dataset2.colNums.length==1) {
	clearColumnSelector();
	hideWorkingMsg(msgCounter);
	return generateRankComparison(dataset1, dataset2, 0, 0);
      }
      else {
	populateColumnSelector(dataset1, dataset2, col1, col2);
	if (isNumeric(col1) && isNumeric(col2))
	  return generateRankComparison(dataset1, dataset2, col1, col2);
	return;
      }
    })
    .finally(()=>hideWorkingMsg(msgCounter));
}

function clearColumnSelector () {
  document.getElementById('columnSelection').replaceChildren();
}

function populateColumnSelector (ds1, ds2, col1, col2) {
  function generateSingleColumnSelector (ds, index, col) {
    if (ds.colNums.length == 1) {
      const input = document.createElement('input');
      input.id = `dsColumn${index}`;
      input.type = 'hidden';
      input.value = 0;
      return input;
    }
    else {
      const label = document.createElement('label');
      label.innerHTML = `Dataset ${index}: `;
      const select = document.createElement('select');
      select.id = `dsColumn${index}`;
      for (let i=0; i<ds.colNums.length; i++) {
	const header = ds.headers[i];
	const colnum = ds.colNums[i];
	const name = (header)? `${header} (column ${colnum})` : `column ${colnum}`;
	const option = new Option(name, i);
	select.add(option);
      }
      select.value = (isNumeric(col)) ? col : ds.colNums.length - 1;
      label.appendChild(select);
      return label;
    }
  }
  const container = document.getElementById('columnSelection');
  const header = document.createElement('div');
  header.innerHTML = "Choose timepoint or column to compare: ";
  const goBtn = document.createElement('button');
  goBtn.innerHTML = "Generate Rank Comparison";
  const ds1Elt = generateSingleColumnSelector(ds1, 1, col1);
  const ds2Elt = generateSingleColumnSelector(ds2, 2, col2);
  container.replaceChildren(header, ds1Elt, ds2Elt, goBtn);
  goBtn.addEventListener('click', function () {
    const ds1Column = document.getElementById('dsColumn1').value;
    const ds2Column = document.getElementById('dsColumn2').value;
    generateRankComparison(ds1, ds2, ds1Column, ds2Column);
  });
}

function generateRankComparison (ds1, ds2, index1, index2) {
  const ranks = computeRanks(ds1, ds2, index1, index2);
  const correlation = spearmanCorrelation(ranks);
  const stdDev = stdDeviation(ranks);
  rankComparison.rankData = ranks;
  rankComparison.correlation = correlation;
  rankComparison.stddev = stdDev;
  rankComparison.orgid = document.getElementById('orgSelect').value;
  rankComparison.valueType = document.getElementById('valueType').value;
  rankComparison.urlType = rankComparison.valueType;
  if (rankComparison.urlType == 'protein') rankComparison.urlType = 'gene';
  rankComparison.ds1 = ds1;
  rankComparison.ds2 = ds2;
  rankComparison.ds1Col = index1;
  rankComparison.ds2Col = index2;
  const stdDev2 = (stdDev*2).toFixed(1);
  const cutoffInput = document.getElementById('outlierCutoff');
  document.querySelector('.stdDev2').innerHTML = stdDev2;
  cutoffInput.value = stdDev2;
  const searchAcPromise = generateAutoCompleteArray(ranks);
  const msgCounter = showWorkingMsg("Uploading Rank Data...");
  registerDeltaRankDataset()
    .then((key)=>{
      populatePToolsVizLinks(key);
      populateDataTable(ranks);
      const rankScatterPlotVega = createRankScatterPlot(ranks, cutoffInput.value);
      let view = new vega.View(vega.parse(rankScatterPlotVega), {
	renderer: 'svg',
	container: '#correlationPlot',
	hover: true
      });
      cutoffInput.addEventListener('change', ()=>view.signal("outlierCutoff", cutoffInput.value).run());
      rankComparison.view = view;
      const searchSelection = document.getElementById('searchSelection');
      const searchInput = document.getElementById('searchTerm');
      const clearBtn = document.getElementById('clearSearch');
      searchAcPromise.then(acArray=>{
	searchInput.addEventListener('change', ()=>{
	  if (!searchInput.value) clearSearch();
	});
	$j(searchInput).autocomplete({
	  source: acArray,
	  change: function (event, ui) {
	    if (!ui.item) clearSearch(true);
	  },
	  select: function (event, ui) {
	    searchInput.value=ui.item.label;
	    searchSelection.value=ui.item.value;
	    doHighlights(ui.item.items);
	    clearBtn.disabled = false;
	    return false;
	  }
	})
	  .autocomplete('instance')._renderItem = function(ul, item) {
	    return $j("<li>")
	      .append (`<div>${item.label} (${item.count})</div>`)
	      .appendTo(ul);
	  };
      });
      document.getElementById('rankComparisonResults').style.display='';
      return view.runAsync()
	.then(()=>{
	  return view.addSignalListener("clickedUrl", (name, url)=>{
	    if (url) window.open(url, "_blank");
	  });
	});
    })
    .finally(()=>hideWorkingMsg(msgCounter));
  populateStats([ { name: "Number of Common Items", value: ranks.length },
		  { name: "Spearman Correlation", value: correlation.toFixed(2) },
		  { name: "Standard Deviation", value: stdDev.toFixed(1) }
		]);
}

function computeRanks (ds1, ds2, index1, index2) {
  const frames = {};
  ds1.data.forEach(entry=>{
    const value = entry.values[index1];
    if (isNumeric(value))
      frames[entry.id] = { id: entry.id,
			   name: entry.name,
			   nameNoHTML: entry.nameNoHTML,
			   v1: value,
			   gene: entry.gene,
			   product: entry.product,
			   complex: entry.heteromultimer,
			   pathway: entry.pathway
			 };
  });
  ds2.data.forEach(entry=>{
    const value = entry.values[index2];
    const record = frames[entry.id];
    if (record && isNumeric(value)) record.v2 = value;
  });
  const comparison = Object.entries(frames)
	.filter(x=>isNumeric(x[1].v2))
	.map(x=>x[1]);
  const values1 = comparison.map(x=>x.v1).sort((a,b)=>a-b);
  const values2 = comparison.map(x=>x.v2).sort((a,b)=>a-b);
  function rankAvg (x, values) {
    const firstIndex = values.indexOf(x);
    const lastIndex = values.lastIndexOf(x);
    if (firstIndex<0 || lastIndex<0)
      throw new Error (`Value ${x} not found in ${values}.`);
    return (firstIndex + lastIndex)/2;
  }
  for (let i=0; i<comparison.length; i++) {
    comparison[i].rank1 = rankAvg(comparison[i].v1, values1);
    comparison[i].rank2 = rankAvg(comparison[i].v2, values2);
    comparison[i].deltaRank = comparison[i].rank1 - comparison[i].rank2;
  }
  return comparison;
}

function spearmanCorrelation (data) {
  const n = data.length;
  if (n < 2) return NaN;
  let sumX = 0;
  let sumY = 0;
  for (const { rank1, rank2 } of data) {
    sumX += rank1;
    sumY += rank2;
  }
  const meanX = sumX / n;
  const meanY = sumY / n;
  let cov = 0;
  let varX = 0;
  let varY = 0;
  for (const { rank1, rank2 } of data) {
    const dx = rank1 - meanX;
    const dy = rank2 - meanY;
    cov += dx * dy;
    varX += dx * dx;
    varY += dy * dy;
  }
  if (varX === 0 || varY === 0) {
    return NaN; // all ranks identical in one variable
  }
  return cov / Math.sqrt(varX * varY);
}

function stdDeviation (data) {
  const diffs = data.map(x=>x.deltaRank);
  return Math.sqrt(diffs.reduce((s,x)=>s+x**2,0)/(diffs.length-1));
}

function createRankScatterPlot(data, outlierCutoff) {
  const max = data.length;
  return {
    "$schema": "https://vega.github.io/schema/vega/v5.json",

    "width": 600,
    "height": 600,
    "padding": {"left": 5, "top": 5, "bottom": 5, "right": 25},

    "data": [
      {
        "name": "points",
        "values": data.map(d => ({
          rank1: d.rank1,
          rank2: d.rank2,
          v1: d.v1,
          v2: d.v2,
          name: rankedObjectNameText(d),
          id: d.id,
	  dRank: d.deltaRank,
          url: makeUrl(d.id)
        }))
      },
      {
	"name": "highlightedPoints",
        "source": "points",
	"transform": [
	  {
	    "type": "filter",
	    "expr": "indexof(highlightedIds, datum.id)>=0"
	  }
	]
      },
    ],

    "signals": [
      {
        "name": "clickedUrl",
        "value": null,
        "on": [
          {
            "events": "@pointMarks:click",
            "update": "datum.url"
          },
	  {
            "events": "@highlightedPointMarks:click",
            "update": "datum.url"
          }
        ]
      },
      {
        "name": "hovered",
        "value": null,
        "on": [
          {
            "events": "@pointMarks:mouseover",
            "update": "datum"
          },
          {
            "events": "@pointMarks:mouseout",
            "update": "null"
          },
	            {
            "events": "@highlightedPointMarks:mouseover",
            "update": "datum"
          },
          {
            "events": "@highlightedPointMarks:mouseout",
            "update": "null"
          }
        ]
      },
      {
	"name": "highlightedIds",
	"value": []
      },
      {
	"name": "outlierCutoff",
	"value": outlierCutoff
      }
    ],

    "scales": [
      {
        "name": "xscale",
        "type": "linear",
        "domain": [0, max],
        "range": "width",
        "nice": true
      },
      {
        "name": "yscale",
        "type": "linear",
        "domain": [0, max],
        "range": "height",
        "nice": true
      }
    ],

    "axes": [
      {
        "scale": "xscale",
        "orient": "bottom",
        "title": "Rank in Dataset 2",
	"grid": true
      },
      {
        "scale": "yscale",
        "orient": "left",
        "title": "Rank in Dataset 1",
	"grid": true
      }
    ],

    "marks": [
      {
	"type": "rule",
	"encode": {
	  "enter": {
	    "x":  { "value": 0 },
	    "y":  { "signal": "height" },
	    "x2": { "signal": "width" },
	    "y2": { "value": 0 },
	    "stroke": { "value": "#888" },
	    "strokeDash": { "value": [5,5] }
	  }
	}
      },
      {
	"type": "rule",
	"encode": {
	  "update": {
	    "opacity": {
              "signal": "hovered ? 1 : 0"
	    },
	    "x": {
              "scale": "xscale",
              "signal": "hovered ? hovered.rank2 : 0"
	    },
	    "y": {"value": 0},
	    "y2": {"signal": "height"},
	    "stroke": {"value": "#888"},
            "strokeWidth": {"value": 2},
	    "strokeDash": {"value": [3,3]}
	  }
	}
      },
      {
	"type": "rule",
	"encode": {
	  "update": {
	    "opacity": {
              "signal": "hovered ? 1 : 0"
	    },
	    "y": {
              "scale": "yscale",
              "signal": "hovered ? hovered.rank1 : 0"
	    },
	    "x": {"value": 0},
	    "x2": {"signal": "width"},
	    "stroke": {"value": "#888"},
            "strokeWidth": {"value": 2},
	    "strokeDash": {"value": [3,3]}
	  }
	}
      },
      {
	"name": "pointMarks",
        "type": "symbol",
        "from": {"data": "points"},
        "encode": {
          "enter": {
            "x": {"scale": "xscale", "field": "rank2"},
            "y": {"scale": "yscale", "field": "rank1"},
            "size": {"value": 50},
            "strokeWidth": {"value": 2},
	    "cursor": {"value": "pointer"},
            "tooltip": {
              "signal": "datum.name"
            }
          },
	  "update": {
	    "fill": 
	      {
		"signal": "hovered == datum ? 'orange' : abs(datum.rank1-datum.rank2)>=outlierCutoff ? 'crimson' : 'steelblue'"
	      },
	    "fillOpacity": {
	      "signal": "hovered == datum ? 1 : 0.6"
	    },
	    "stroke": {
	      "signal": "hovered == datum ? 'black' : null"
	    }
	  }
        }
      },
      {
	"name": "highlightedPointMarks",
        "type": "symbol",
        "from": {"data": "highlightedPoints"},
	"encode": {
          "enter": {
            "x": {"scale": "xscale", "field": "rank2"},
            "y": {"scale": "yscale", "field": "rank1"},
            "size": {"value": 50},
	    "fill": {"value": "lime"},
            "strokeWidth": {"value": 2},
	    "cursor": {"value": "pointer"},
            "tooltip": {
              "signal": "datum.name"
            }
          },
	  "update": {
	    "stroke": {
	      "signal": "hovered == datum ? 'black' : null"
	    }
	  },
        },
      },
      {
	"type": "text",
	"encode": {
	  "update": {
	    "opacity": {
              "signal": "hovered ? 1 : 0"
	    },
	    "x": {
              "scale": "xscale",
              "signal": "hovered ? hovered.rank2 : 0"
	    },
	    "y": {
              "value": -5
	    },
	    "text": {
              "signal": "hovered ? format(hovered.rank2, ',') : ''"
	    },
	    "align": {"value": "center"},
	    "fontWeight": {"value": "bold"}
	  }
	}
      },
      {
	"type": "text",
	"encode": {
	  "update": {
	    "opacity": {
              "signal": "hovered ? 1 : 0"
	    },
	    "x": {
              "signal": "width + 10"
	    },
	    "y": {
              "scale": "yscale",
              "signal": "hovered ? hovered.rank1 : 0"
	    },
	    "text": {
              "signal": "hovered ? format(hovered.rank1, ',') : ''"
	    },
	    "align": {"value": "left"},
	    "baseline": {"value": "middle"},
	    "fontWeight": {"value": "bold"}
	  }
	}
      },
      {
	"type": "group",
	"encode": {
	  "update": {
	    "x": {"signal": "width - 570"},
	    "y": {"value": -106},
	    "opacity": {
              "signal": "hovered ? 1 : 0"
	    }
	  }
	},
	"marks": [
	  {
	    "type": "rect",
	    "encode": {
              "enter": {
		"width": {"value": 560},
		"height": {"value": 86},
		"fill": {"value": "white"},
		"fillOpacity": {"value": 0.9},
		"stroke": {"value": "#999"},
		"cornerRadius": {"value": 4}
              }
	    }
	  },
	  {
	    "type": "text",
	    "encode": {
              "update": {
		"x": {"value": 8},
		"y": {"value": 18},
		"fontWeight": {"signal": "hovered ? 'bold' : 'null'"},
		"text": {
		  "signal": "hovered ? hovered.name : 'Hover over any point to see its data...'"
		},
		"fill": {
		  "signal": "hovered ? 'black' : 'gray'"
		}
              }
	    }
	  },
	  {
	    "type": "text",
	    "encode": {
              "update": {
		"x": {"value": 8},
		"y": {"value": 40},
		"text": {
		  "signal": "hovered ? 'Dataset 1: Value: ' + hovered.v1 + ', Rank: '+ hovered.rank1 : ''"
		}
              }
	    }
	  },
	  {
	    "type": "text",
	    "encode": {
              "update": {
		"x": {"value": 8},
		"y": {"value": 58},
		"text": {
		  "signal": "hovered ? 'Dataset 2: Value: ' + hovered.v2 + ', Rank: '+ hovered.rank2 : ''"
		}
              }
	    }
	  },
	  {
	    "type": "text",
	    "encode": {
              "update": {
		"x": {"value": 8},
		"y": {"value": 76},
		"fontWeight": {"value": "bold"},
		"text": {
		  "signal":
		  "hovered ? '\u0394Rank: ' + format(hovered.rank1 - hovered.rank2, '+,') : ''"
		},
		"fill": [
		  {
		    "test": "hovered && hovered.rank1 > hovered.rank2",
		    "value": "firebrick"
		  },
		  {
		    "value": "forestgreen"
		  }
		]
              }
	    }
	  }
	]
      }
    ]
  };
}

function registerDeltaRankDataset () {
  const data = rankComparison.rankData;
  const stdDev = rankComparison.stdDev;
  const orgid = rankComparison.orgid;
  const valueTypeSelector = document.getElementById('valueType');
  const vType = valueTypeSelector.value;
  const vTypeName = valueTypeSelector.options[valueTypeSelector.selectedIndex].innerHTML;
  const cachedComparison = getCachedComparison(rankComparison.ds1.datakey,
					       rankComparison.ds2.datakey,
					       rankComparison.ds1Col,
					       rankComparison.ds2Col);
  rankComparison.title = document.getElementById('comparisonName').value;
  const title =  rankComparison.title ||
	(cachedComparison && cachedComparison.title) ||
	`${vTypeName} dRank(Dataset1 - Dataset2)`;
  if (cachedComparison) {
    rankComparison.datakey = cachedComparison.dsRankKey;
    if (title != cachedComparison.title) {
      cachedComparison.title = title;
      updateStoredExptTitle(cachedComparison.dsRankKey, title);
      updateCachedComparisons();
    }
    document.getElementById('comparisonName').value = title;
    return Promise.resolve(cachedComparison.dsRankKey);
  }
  else {
    const msgCounter = showWorkingMsg("Uploading rank data to PathwayTools...");
    let datatext = "$Frame\tdeltaRank\n";
    for (let i=0; i<data.length; i++) {
      datatext += data[i].id + '\t' + data[i].deltaRank + '\n';
    }
    const colors = ["#a136fb","#2f6bd3","#7d7d7d","#db8b1d","#f33005"];
    const cutoffs = stdDev && [ -(stdDev*2), -stdDev, stdDev, stdDev*2];
    const params = new FormData();
    params.append('class', vType);
    params.append('orgid', orgid);
    params.append('title', title);
    params.append('datacolumns', 1);
    params.append('expressiontype', 'relative');
    params.append('log', 'on');
    params.append('datatext', datatext);
    if (cutoffs) {
      params.append('colors', colors);
      params.append('cutoffs', cutoffs);
    }
    const url = `${ptoolsBaseUrl}/register-omics-dataset`;
    return fetch(url, { method: 'POST', body: params })
      .then(response=>response.json())
      .then(result=>{
	if (!result.success) throw new Error(result.error);
	rankComparison.datakey = result.key;
	saveOmics(params, result);
	document.getElementById('comparisonName').value = result.title;
	cacheComparison(rankComparison);
	return result.key;
      })
      .catch((error)=>{
	alert('Upload of rank comparison to Pathway Tools failed: '+error);
	return null;
      })
      .finally(()=>hideWorkingMsg(msgCounter));
  }
}

function populatePToolsVizLinks (datakey) {
  function createLink(href, text) {
    const link = document.createElement('a');
    link.classList.add('ptoolsLink');
    if (href) link.target = '_blank';
    link.href = href;
    link.innerHTML = text;
    return link;
  }
  const linksDiv = document.getElementById('links');
  if (datakey) {
    const orgid = document.getElementById('orgSelect').value;
    const celovLink = createLink(`${ptoolsBaseUrl}/overviewsWeb/celOv.shtml?orgid=${orgid}&datakey=${datakey}`, "Overlay rank differences on the Cellular Overview");
    const dashboardLink = createLink(`${ptoolsBaseUrl}/dashboard/dashboard.html?dataset=${datakey}`, "Visualize rank differences in the Omics Dashboard");
    const pwyTableLink = createLink(`${ptoolsBaseUrl}/${orgid}/overview-expression-map?datakey=${datakey}&display=table&tablethreshold=20`, "Show a table of the 20 pathways with the greatest variance");
    const topNLink = createLink(null, "Compare lists of top N most abundant entities");
    topNLink.href = 'javascript:$j("#topNPanel").dialog("open")';
    linksDiv.replaceChildren(celovLink, dashboardLink, pwyTableLink, topNLink);
  }
  else linksDiv.replaceChildren();
}

function populateStats (stats) {
  const div = document.getElementById('stats')
  const table = document.createElement("table");
  table.classList.add('statsTable');
  stats.forEach(stat=>{
    const row = document.createElement("tr");
    const th = document.createElement("th");
    const td = document.createElement("td");
    th.innerHTML = stat.name + ": ";
    td.innerHTML = stat.value;
    row.replaceChildren(th, td);
    table.appendChild(row);
  });
  div.replaceChildren(table);
}

function populateDataTable (data) {
  const tableSelector = '#outlierTable .contents';
  const vType = rankComparison.valueType;
  if (!rankComparison.table && $j.fn.dataTable.isDataTable(tableSelector))
    rankComparison.table = $j(tableSelector).DataTable();
  if (rankComparison.table) {
    rankComparison.table.clear()
      .columns('.defaultHide').visible(false)
      .columns(`.${vType}`).visible(true)
      .rows.add(data)
      .draw();
  }
  else {
    const table = generateDataTable(tableSelector, data, '600px');
    rankComparison.table = table;
    table.search.fixed('range', function (searchStr, data, index) {
      let cutoff = document.getElementById('outlierCutoff').value;
      let absDelta = Math.abs(parseFloat(data.deltaRank)) || 0;
      return absDelta >= cutoff;
    });
    rankComparison.table.columns(`.${vType}`).visible(true)
      .columns('.pathway').visible(true)
      .draw();
    document.getElementById('outlierCutoff').addEventListener('change', ()=>table.order([[9,'desc']]).draw());
  }
}

function resetOutlierCutoff () {
  const stdDev2 = (rankComparison.stddev*2).toFixed(1);
  const elt = document.getElementById('outlierCutoff');
  elt.value = stdDev2;
  elt.dispatchEvent(new Event('change', {bubbles: true}));
}

function clearSearch (keepSearchTerm) {
  document.getElementById('searchSelection').value='';
  if (!keepSearchTerm) document.getElementById('searchTerm').value= '';
  rankComparison.view?.signal("highlightedIds", []).run();
  document.getElementById('clearSearch').disabled = true;
  rankComparison.highlightTable?.clear();
  document.getElementById('highlightTableContainer').style.visibility = 'hidden';
}

function doHighlights (highlightIds) {
  rankComparison.view.signal("highlightedIds", highlightIds).run();
  const highlightData = rankComparison.rankData.filter(x=>highlightIds.find(id=>x.id == id));
  if (highlightData.length) {
    document.getElementById('highlightTableContainer').style.visibility = '';
    const tableSelector = '#highlightTable';
    const vType = rankComparison.valueType;
    if (!rankComparison.highlightTable && $j.fn.dataTable.isDataTable(tableSelector))
      rankComparison.highlightTable = $j(tableSelector).DataTable();
    if (rankComparison.highlightTable) {
      rankComparison.highlightTable.clear()
	.columns('.defaultHide').visible(false)
	.columns(`.${vType}`).visible(true)
	.rows.add(highlightData)
	.draw();
    }
    else {
      rankComparison.highlightTable = generateDataTable(tableSelector, highlightData, '360px')
	.columns(`.${vType}`).visible(true)
	.draw();
    }
  }
}

function makeUrl (id, urlType) {
  if (!urlType) urlType = rankComparison.urlType;
  let url = `${ptoolsBaseUrl}/${urlType}?orgid=${rankComparison.orgid}&id=${id}`;
  if (rankComparison.datakey) url += `&datakey=${rankComparison.datakey}`;
  return url;
}

function rankedObjectNameText (rankedObject) {
  const name = rankedObject.nameNoHTML;
  const geneOrProd = (rankedObject.gene && rankedObject.gene[0]) || (rankedObject.product && rankedObject.product[0]);
  if (geneOrProd)
    return name+' ('+removeHtmlTags(geneOrProd.name)+')';
  else return name;
}

function generateAutoCompleteArray (dataset) {
  const valueType = document.getElementById('valueType').value;
  const valueTypeMapping = { gene: "All-Genes",
			     protein: "Proteins",
			     reaction: "Reactions",
			     compound: "Compounds"
			   };
  const dataClass = valueTypeMapping[valueType];
  const url = `${ptoolsBaseUrl}/ajax-get-dashboard-data?orgid=${orgId()}&dataset=empty&baseClass=${dataClass}`;
  return fetch(url)
    .then(response=>response.json())
    .then(data=>{
      generateAllWidgetData(data);
      const dict = buildCategoryDictionary(data);
      const entityData = data.rawdata;
      const acArray = [];
      for (let datakey in entityData) {
	const origEntry = dataset.find(x=>x.id==datakey);
	if (origEntry) {
	  const names = [];
	  const attrs = entityData[datakey];
	  function addName (x) {
	    if (x && !names.find(v=>v.toLowerCase()==x.toLowerCase())) names.push(x);
	  }
	  addName(attrs.name);
	  if (origEntry.gene?.length)
	    origEntry.gene.forEach(g=>{
	      addName(g.name);
	      addName(g.id);
	    });
	  if (origEntry.product?.length)
	    origEntry.product.forEach(p=>{
	      addName(p.name);
	      addName(p.id);
	    });
	  addName(attrs.shortName);
	  addName(attrs.auxText);
	  addName(datakey);
	  names.forEach(n=>acArray.push({ label: n,
					  value: datakey,
					  count: 1,
					  items: [datakey]
					}));
	}
      }
      for (let category in dict) {
	const attrs = dict[category];
	const items = attrs.entities.filter(key=>dataset.find(x=>x.id==key));
	if (items.length) {
	  const names = [];
	  function addName (x) {
	    if (x && !names.find(v=>v.toLowerCase()==x.toLowerCase())) names.push(x);
	  }
	  addName(attrs.name);
	  addName(category);
	  names.forEach(n=>acArray.push({ label: n,
					  value: category,
					  count: items.length,
					  items: items
					}));
	}
      }
      return acArray;
    });
}

function generateDataTable (tableSelector, data, scrollY, disableSearch, topText) {
  const options = {
    data: data,
    paging: false,
    scrollCollapse: true,
    scrollY: scrollY,
    searching: !disableSearch,
    columns: [
      { data: 'id',
	orderable: false,
	title: "<a download class='tableDownload' title='Download Table as TSV' aria-label='Download Table as TSV'><i class='fa fa-download'></i></a> ID",
	render: function (data, type, row, meta) {
	  if (type == 'display')
	    return `<a href="${makeUrl(data)}" target=_blank>${data}</a>`;
	  else return data;
	}
      },
      { data: 'name',
	title: 'Name'
      },
      { data: 'gene',
	title: 'Gene',
	render: data=>data?.map(x=>x.name).join(", ") || '',
	visible: false,
	className: 'protein defaultHide',
	defaultContent: ''
      },
      { data: 'product',
	title: 'Product',
	render: data=>data?.map(x=>x.name).join(", ") || '',
	visible: false,
	className: 'gene defaultHide',
	defaultContent: ''
      },
      { data: 'v1',
	title: 'Dataset 1 Value'
      },
      { data: 'v2',
	title: 'Dataset 2 Value'
      },
      { data: 'rank1',
	title: 'Dataset 1 Rank'
      },
      { data: 'rank2',
	title: 'Dataset 2 Rank'
      },
      { data: 'deltaRank',
	title: '&Delta;Rank'
      },
      { data: 'deltaRank',
	title: '|&Delta;Rank|',
	render: data=>Math.abs(parseFloat(data)),
	visible: false
      },
      { data: null,
	title: 'Pathway or Complex',
	render: row=>(row.pathway?.length && objectsToLinks(row.pathway, 'pathway')) || objectsToLinks(row.complex, 'complex'),
	visible: false,
	className: 'pathway',
	defaultContent: ''
      },

      ],
    order: [[9, 'desc']],
  };
  if (disableSearch || topText) {
    options.layout = {};
    if (disableSearch) options.layout.bottomStart = null;
    if (topText)
      options.layout.topStart = {
	div: {
	  className: 'tableTopText',
	  html: topText
	}
      };
  }

  const table = new DataTable(tableSelector, options);
  $j(table.table().container()).on('click', '.tableDownload', function (e) {
    e.stopPropagation();
    const tbody = table.table().container().querySelector('.dt-scroll-body table');
    this.href=tableToDataURI(tbody);
  });
  table.on('mouseover', 'tbody tr', function () {
    const id = table.row(this).data().id;
    const point = rankComparison.view.data("points").find(x=>x.id==id);
    rankComparison.view.signal('hovered', point).run();
  });
  table.on('mouseout', 'tbody tr', function () {
    let id = table.row(this).data();
    rankComparison.view.signal('hovered', null).run();
  });
  return table;
}

function updateTopNTables () {
  generateTopNTables(rankComparison.rankData,
		     document.getElementById('topNCutoff').value,
		     document.getElementById('topNTables')
		    );
}

function generateTopNTables (data, n, parent) {
  const rankCutoff = data.length - n;
  const both = data.filter(x=>x.rank1 >= rankCutoff && x.rank2 >= rankCutoff);
  const only1 = data.filter(x=>x.rank1 >= rankCutoff && x.rank2 < rankCutoff);
  const only2 = data.filter(x=>x.rank1 < rankCutoff && x.rank2 >= rankCutoff);
  const vType = rankComparison.valueType;
  function createOrUpdateTable (id, caption, data) {
    let elt = document.getElementById(id);
    if (!elt) {
      elt = document.createElement('table');
      elt.id = id;
      elt.classList.add('topNTable');
      elt.classList.add('display');
      elt.classList.add('compact');
      elt.classList.add('cell-border');
      parent.appendChild(elt);
    }
    const selector = '#'+id;
    if ($j.fn.dataTable.isDataTable(selector)) {
      const table = $j(selector).DataTable();
      table.clear()
	.rows.add(data)
	.draw();
      table.table().container().querySelector('.tableTopText').innerHTML = caption;
      return table;
    }
    else {
      const table = generateDataTable(selector, data, '300px', true, caption)
	    .columns(`.${vType}`).visible(true)
	    .draw();
      return table;
    }
  }
  createOrUpdateTable('topNOnly1', `Most abundant in Dataset 1 but not Dataset 2 (${only1.length})`, only1);
  createOrUpdateTable('topNOnly2', `Most abundant in Dataset 2 but not Dataset 1 (${only2.length})`, only2);
  createOrUpdateTable('topNBoth', `Most abundant in both Dataset 1 and Dataset 2 (${both.length})`, both);
}



function objectsToLinks (data, urlType) {
  return data?.map(x=>`<a href='${makeUrl(x.id, urlType)}' target=_blank>${x.name}</a>`).join(', ');
}

function getCachedComparisons () {
  const localComparisons = JSON.parse(localStorage.getItem('rankComparisons'));
  if (localComparisons?.length) {
    const localDatasets = allLocalOmicsDatasets();
    return localComparisons.filter(c=>localDatasets.find(x=>x.key==c.ds1Key) && localDatasets.find(x=>x.key==c.ds2Key) && localDatasets.find(x=>x.key==c.dsRankKey))
  }
  return [];
}

function cacheComparison (comparison) {
  const params = {
    orgid: comparison.orgid,
    valueType: comparison.valueType,
    ds1Key: comparison.ds1.datakey,
    ds1Col: comparison.ds1Col,
    ds2Key: comparison.ds2.datakey,
    ds2Col: comparison.ds2Col,
    dsRankKey: comparison.datakey,
    title: comparison.title
  }
  cachedComparisons.push(params);
  updateCachedComparisons();
  return cachedComparisons;
}

function getCachedComparison (ds1Key, ds2Key, ds1Col, ds2Col) {
  return cachedComparisons?.find(c=>c.ds1Key==ds1Key&&c.ds2Key==ds2Key&&c.ds1Col==ds1Col&&c.ds2Col==ds2Col);
}

function updateCachedComparisons () {
  localStorage.setItem('rankComparisons', JSON.stringify(cachedComparisons));
  populatePrevComparisonSelector();
}

function populatePrevComparisonSelector () {
  const container = document.getElementById('restorePrev');
  const comparisonSelector = document.getElementById('comparisonId');
  if (cachedComparisons?.length) {
    comparisonSelector.replaceChildren();
    comparisonSelector.add(new Option("--Select a Comparison--", ""));
    cachedComparisons.sort(alphaSort('title')).forEach(params=>
      comparisonSelector.add(new Option(params.title || "(untitled) "+params.dsRankKey, params.dsRankKey)));
    container.style.display = '';
    comparisonSelector.selectedIndex = 0;
  }
  else container.style.display = 'none';
}


function initPrevComparisonSelector () {
  cachedComparisons = getCachedComparisons();
  populatePrevComparisonSelector();
  const comparisonSelector = document.getElementById('comparisonId');
  comparisonSelector.addEventListener('change', function () {
    const key = comparisonSelector.value;
    const params = key && cachedComparisons.find(x=>x.dsRankKey == key);
    if (params) {
      const orgSelector = document.getElementById('orgSelect');
      const vTypeSelector = document.getElementById('valueType');
      if (orgSelector.value != params.orgid || vTypeSelector.value != params.valueType) {
	orgSelector.value = params.orgid;
	vTypeSelector.value = params.valueType;
	populateDatakeySelectors();
      }
      document.getElementById('comparisonName').value = params.title || "";
      document.querySelectorAll("input[name^='source_'][value='exp']:not(:checked)")
	.forEach(elt=>{
	  elt.checked = true;
	  enableSource(elt, (elt.name == 'source_1') ? 1 : 2);
	});
      const dsSelector1 = document.getElementById('datakeySelector_1');
      dsSelector1.value = params.ds1Key;
      omicsSrcChanged(dsSelector1);
      const dsSelector2 = document.getElementById('datakeySelector_2');
      dsSelector2.value = params.ds2Key;
      omicsSrcChanged(dsSelector2);
      retrieveCompareData(params.ds1Col, params.ds2Col);
    }
  });
}

function deleteComparison (dsRankKey) {
  if (!dsRankKey) dsRankKey = rankComparison.datakey;
  if (dsRankKey == rankComparison.datakey) {
    rankComparison = {};
    document.getElementById('rankComparisonResults').style.display = 'none';
  }
  cachedComparisons = cachedComparisons.filter(x=>x.dsRankKey != dsRankKey);
  updateCachedComparisons();
}

function deleteAllComparisons () {
  cachedComparisons = [];
  updateCachedComparisons();
  rankComparison = {};
  document.getElementById('rankComparisonResults').style.display = 'none';
}




/*************** PanelConfigs *******************/

/// panelConfigs is the overall mapping table that is kept in the Browser's localStorage.
/// Its keys are title strings, and its values are objects that contain the attributes of
/// of each vizDiv for a given panelConfigName .
let panelConfigs = {};

/// To remove ALL the panelConfigs entries, call this:
///
function clearPanelConfigs () {
    localStorage.removeItem("panelConfigs")
    panelConfigs = {}
}

function panelConfigsRetrieval () {
    if ( isEmpty(panelConfigs) ) {
	const rawPanelConfigs = localStorage.getItem('panelConfigs')
	if (!rawPanelConfigs) {
	    alert("panelConfigs are not yet set up.")
	} else {
	    // Update and store in the global variable
	    panelConfigs = JSON.parse(rawPanelConfigs);

            ///const allCachedDatakeys = JSON.parse(localStorage.getItem('omicsDatasets')).map(x=>x.key);

	    // populate the dropdown menu too
	    for (const key of Object.keys(panelConfigs)) {
		addPanelConfigNameDropdownOption (key, false)
		//console.log(key);
	    }
	}
    }
    return panelConfigs
}

/// Given a panelConfigName string in the panelConfigName text entry box, return the corresponding object that contains
/// the attributes of the vizDiv info.  Will initialize values for new names with empty objects.
///
function panelConfigObjRetrieval (panelConfigName) {
    panelConfigsRetrieval()
    if ( isEmpty(panelConfigs[panelConfigName]) ) {
	// Create an empty placeholder object to return
	panelConfigs[panelConfigName] = {}
	localStorage.setItem('panelConfigs', JSON.stringify(panelConfigs))
    }
    else {
	//console.dir( panelConfigs );
	let panelConfig = panelConfigs[panelConfigName]
	//console.log(panelConfig)
	if ( isEmpty(panelConfig) ) {
	    // Create an empty placeholder object to return
	    panelConfig[panelConfigName] = {}
	    localStorage.setItem('panelConfigs', JSON.stringify(panelConfigs))
	}
    }
    return panelConfigs[panelConfigName]
}

/// vizDivTitle strings,
/// Given a vizDivTitle string and the panelConfig object,
/// return the corresponding object that will contain the attributes of a vizDiv
function vizDivConfigRetrieval (vizDivTitle, panelConfigObj) {
    if ( isEmpty(panelConfigObj[vizDivTitle]) ) {
	// Create an empty placeholder object to return
	panelConfigObj[vizDivTitle] = {}
	//alert('in vizDivConfigRetrieval: was empty for vizDivTitle: ' + vizDivTitle)
	localStorage.setItem('panelConfigs', JSON.stringify(panelConfigs))
    }
    return panelConfigObj[vizDivTitle]
}

/*
vizDivConfigRetrieval('rty')
<- Object {  }

panelConfigs
<- Object { rty: {} }
 */

function vizDivConfigStoreDragCoords (titleDiv, ui) {
    // Supposedly, these are relative to the entire browser window !
    //let absoluteLeft = ui.offset.left;
    //let absoluteTop  = ui.offset.top;
    let absoluteLeft = ui.position.left;
    let absoluteTop  = ui.position.top;
    console.log("dragged: new Absolute Position -> Left: " + absoluteLeft + ", Top: " + absoluteTop);
    console.log("dragged: title: " + titleDiv.textContent ); // titleDiv.innerHTML
    //console.dir(ui)
    // the currently selected panelConfigName
    const panelConfigObj = currentPanelConfigObj()
    let vizDivConfig = vizDivConfigRetrieval(titleDiv.textContent, panelConfigObj)
    vizDivConfig['x'] = absoluteLeft
    vizDivConfig['y'] = absoluteTop
    //vizDivConfig['absoluteLeft'] = absoluteLeft
    //vizDivConfig['absoluteTop']  = absoluteTop
    
    // Save to localStorage (must be stringified)
    localStorage.setItem('panelConfigs', JSON.stringify(panelConfigs))
}

function vizDivConfigStoreResizeCoords (titleDiv, ui) {
    let width = ui.size.width;
    let height  = ui.size.height;
    //console.log("resized: new Position -> width: " + width + ", height: " + height);
    //console.log("resized: title: " + titleDiv.textContent ); // titleDiv.innerHTML
    //console.dir(ui)
    // the currently selected panelConfigName
    const panelConfigObj = currentPanelConfigObj()
    let vizDivConfig = vizDivConfigRetrieval(titleDiv.textContent, panelConfigObj)
    vizDivConfig['width']  = width
    vizDivConfig['height'] = height

    // Save to localStorage (must be stringified)
    localStorage.setItem('panelConfigs', JSON.stringify(panelConfigs))
}


function isEmpty(obj) {
  for (let key in obj) {
    if (Object.hasOwn(obj, key)) return false;
  }
  return true;
}

    //  let vizDivSlots = panelConfigs[vizDivTitle]
    // Save to localStorage (must be stringified)
    //localStorage.setItem('vizDiv_position', JSON.stringify(coords));

///kr:Jun-24-2026 This collects all the wgDiv diagrams on the page and stores their essential info
/// such that the overall panel could be re-constructed in the future.
///
function recordAllVizDivConfigs() {
    document.querySelectorAll('div.wgContainer').forEach(elt=>{

	const wgDiv = elt.querySelector('canvas').parentNode
	const divId = wgDiv.dataset.originalId
	//const titleDiv = elt.querySelector('titleBar')
	const titleDiv = elt.getElementsByClassName('titleBar')[0] // there should be only 1
	const title = titleDiv.textContent  // or .innerText ?
	const href  = titleDiv.firstChild // the whole <a href= ...> construct
	//const href  = titleDiv.childNodes[0] // the whole <a href= ...> construct
	// structure under panelConfigs :
	//panelConfigs["tst 1"]["phenylacetate degradation I (aerobic) (Pathways)"]["href"]
	//==>    <a href="/pathway?orgid=ECOLI&id=PWY0-321" target="_blank">

	//console.dir( wgDiv )
	//console.dir( titleDiv )
	//console.log( titleDiv.textContent )
	//console.log('href: ' + href + ' title: ' + title + ' wgDiv: ' + wgDiv)
	  //  div.dataset.frameid = pwyId;
	const pwyId = wgDiv.dataset.frameid
	//const displayType = wgDiv.dataset.displaytype
	populatePanelConfig(wgDiv, divId, pwyId, title, href, titleDiv)

	console.log('recordAllVizDivConfigs is saving panelConfigs to localStorage')
	localStorage.setItem('panelConfigs', JSON.stringify(panelConfigs))

    }
							)
}


/// For preventing duplicate entries
///
function checkSelectorValueExists(selectId, targetValue) {
    // Escaping targetValue prevents syntax errors if the string contains quotes
    const safeValue = CSS.escape(targetValue)
    //const safeValue = targetValue
    const match = document.querySelector(`#${selectId} option[value="${safeValue}"]`)
    return match != null
}

function createNewConfigBtnHandler () {
    // Get the content of the text entry box
    const newConfigNameStr = document.getElementById("newConfigName").value
    if ( newConfigNameStr == ''  || checkSelectorValueExists('panelConfigName', newConfigNameStr) ) {
	alert('Please enter a new, unique name')
    }
    else {
	addPanelConfigNameDropdownOption (newConfigNameStr, true)
	// Erase the text that was entered
	document.getElementById("newConfigName").value = ""
	// Grab and store the existing configs of wgDivs displayed, if any
	recordAllVizDivConfigs()
    }
    //console.log('Create new Config : ' +  newOption.text);
}

/// This adds another entry to the dropdown menu.  When selectNowP is true,
/// the selector then shows the new entry right away.
///
function addPanelConfigNameDropdownOption (newConfigNameStr, selectNowP) {
    const panelConfigNameDropdown = document.querySelector('#panelConfigName')
    // Create the new option element
    const newOption = document.createElement('option')
    // Store the value and text
    newOption.value = newConfigNameStr; // The hidden value stored programmatically
    newOption.text  = newConfigNameStr; // The text the user sees
    // Append it to the dropdown menu
    panelConfigNameDropdown.add(newOption);
    if (selectNowP == true) {
	// Force the dropdown to select this new option immediately
	panelConfigNameDropdown.value = newOption.value;
    }
}

function populatePanelConfig(wgDiv, divId, pwyId, title, href, titleDiv) {
    // the currently selected panelConfigName
    const panelConfigObj = currentPanelConfigObj()
    const vizDivConfig = vizDivConfigRetrieval (title, panelConfigObj)

    vizDivConfig['vizContentId'] = divId // example for a pwy: ECOLI_HOMOSER-THRESYN-PWY
    vizDivConfig['pwyId'] = pwyId        // example for a pwy: HOMOSER-THRESYN-PWY
    vizDivConfig['title'] = title        // shown in the top bar of the wgsnap window
    vizDivConfig['href'] = href          // used also in the top bar of the wgsnap window
    vizDivConfig['displayType'] = wgDiv.dataset.displaytype
    console.log('populate: displayType: ' + vizDivConfig['displayType'])

    //const wgDivParent = wgDiv.parentNode
    //console.dir (wgDivParent)

    // DOMRect coords are relative to the top-left corner of the browser's entire viewport !
    const allPanelVizContainerDiv = document.getElementById("panelVisualizations"); // in panelConfigs.html
    const visualizationsDomrect = allPanelVizContainerDiv.getBoundingClientRect()
    
    const domrect = wgDiv.getBoundingClientRect()
    const titleDomrect = titleDiv.getBoundingClientRect()
    //vizDivConfig['x'] = domrect.x
    vizDivConfig['x'] = titleDomrect.x
    console.log('populate: titleDomrect.x: ' + titleDomrect.x + ' domrect.x: ' + domrect.x)
    // To obtain a y that is RELATIVE to the top of panelVisualizations, this setoff
    // has to be subtracted.  domrect.y generally will be the larger number (further down on the page).
    vizDivConfig['y'] = titleDomrect.y - visualizationsDomrect.y
    vizDivConfig['width'] = domrect.width
    vizDivConfig['height'] = domrect.height
    
    //console.log('populate: divId: ' + divId + '  title: ' + title + " y: " + domrect.y)
    //console.log('populate: visualizationsDomrect.y: ' + visualizationsDomrect.y + " adjusted y: " + vizDivConfig['y'])

    //console.log('vizDivConfig: ' + vizDivConfig)
}

/// the currently selected panelConfigName
function currentPanelConfigObj () {
    const panelConfigNameDropdown = document.querySelector('#panelConfigName')
    console.log('panelConfigName: ' + panelConfigNameDropdown.value)
    const panelConfigObj = panelConfigObjRetrieval(panelConfigNameDropdown.value)
    return panelConfigObj
}

function changedPanelConfigHandler () {
    //const panelConfigNameDropdown = document.querySelector('#panelConfigName')
    //const currentNewConfigNameStr = panelConfigNameDropdown.value
    //alert('currentNewConfigNameStr: ' + currentNewConfigNameStr)

    const panelConfigObj = currentPanelConfigObj()
    //alert('panelConfigObj: ' + panelConfigObj)
    switchToChangedPanelConfig(panelConfigObj)
}

/// Destructively remove the current panelConfig entry
/// after a confirmation yes/no popup
function deleteCurrentPanelConfig() {
    const panelConfigNameDropdown = document.querySelector('#panelConfigName')
    const currentNewConfigNameStr = panelConfigNameDropdown.value
    //console.log('currentNewConfigNameStr: ' + currentNewConfigNameStr)
    if (confirm('Really delete the Config: ' + currentNewConfigNameStr + ' ?') ) {
	// 1. Get the index of the currently highlighted option
	const currentIndex = panelConfigNameDropdown.selectedIndex;
	//console.log('currentIndex: ' + currentIndex)
	//console.log('currentNewConfigNameStr by Index: ' + panelConfigNameDropdown.options[currentIndex].value )
	// Destructively remove from the selector, the panelConfigs object, and its localStorage version
	panelConfigNameDropdown.remove(currentIndex)
	delete panelConfigs[currentNewConfigNameStr]
	// Save to localStorage (must be stringified)
	localStorage.setItem('panelConfigs', JSON.stringify(panelConfigs))
    }
}

function deleteDiagramInCurrentConfig(title) {
    const configObj = currentPanelConfigObj()
    console.log('deleting Diagram: ' + title)
    delete configObj[title]
    // Save to localStorage (must be stringified)
    localStorage.setItem('panelConfigs', JSON.stringify(panelConfigs))
}

function switchToChangedPanelConfig () {
    // First, clear out any existing diagrams from the previous panelConfig
    document.querySelectorAll('div.wgContainer').forEach(div=> div.remove() )
    // Then, populate with new diagrams for the current panelConfig
    for (const [title, vizDivConfig] of Object.entries( currentPanelConfigObj() )) {
	//console.log(`${title}: ${vizDivConfig}`);
	//console.log('retrieving title: ' + title)
	//console.dir(vizDivConfig)
	console.log('displayType: ' + vizDivConfig['displayType'])
	const displayType = vizDivConfig['displayType']
	if (displayType == 'pwyWg') {
	    const pwyId = vizDivConfig['pwyId']
	    const pwyName = title
	    //const divId = vizDivConfig['vizContentId']
	    //const href  = vizDivConfig['href']
	    //alert('vizDivConfig divId: ' + divId + ' pwyName: ' + pwyName)

	    // suppressPanelConfigP is true
	    pwyBtnHandler (true, true, vizDivConfig)
	    // The arguments  datakey, compareKeys  are not supplied (and therefore false)
	    // suppressPanelConfigP is true
	//    displayDataOnPathwayDiagramInternal(pwyId, pwyName, false, false, true, vizDivConfig)
	    // .then(div=>{
	    //	console.log('div: ' + div)
	    //	moveVizDivToCoords (div, vizDivConfig)
	    // })
	}
	else if (displayType == 'celovWg') {
	    //displayDataOnCelOvDiagram (datasetInfoArray, compareDatasets)
	    //displayDataOnCelOvDiagram (false, false)
	    // suppressPanelConfigP is true
	    celovBtnHandler(true, true, vizDivConfig)
	}
	console.log('### done with retrieving : ' + title)
    }

    // If any dataset was selected, add it now
    //updateExistingDisplaysBtnHandler() // If there is no accessible data feed, this hit a hard return!
    silentUpdateExistingDisplaysBtnHandler()
}

///kr:Jul-14-2026 By now, this is a No-op, and the coords are assigned in generateWgDiv()
/// when vizDivConfig is passed in...
function moveVizDivToCoords (div, vizDivConfig) {
    const canvas = div.querySelector('canvas');
    //console.dir(canvas)
    
    const x = vizDivConfig['x']
    const y = vizDivConfig['y']
    const width  = vizDivConfig['width']
    const height = vizDivConfig['height']
    console.log('moveVizDivToCoords  x: ' + x + ' y: ' + y)
    
      //popupDiv.parentElement.style.height = newHeight+'px';
    canvas.style.width  = width+'px';
    canvas.style.height = height+'px';
    //canvas.parentElement.style.width = '';
    //canvas.parentElement.style.height = '';

    canvas.style.left = x+'px'
    canvas.style.top  = y+'px'
    //canvas.parentElement.style.x = '';
    //canvas.parentElement.style.y = '';
  
}

