# DEACTIVATED: This module previously provided a Marimo-based data exploration
# UI that depended on the vecoli package (vEcoli Python library). The vecoli
# dependency has been removed from the project. This file is retained for
# reference but is not imported or executed by any active code path.
#
# To reactivate, install vecoli and restore the viva_api.data.data_service module.

raise SystemExit("app/ui/explore.py is deactivated — see comment at top of file")

import marimo  # noqa: E402

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    # /// script
    # [tool.marimo.display]
    # theme = "dark"
    # ///
    return


@app.cell
def _():
    from viva_api.config import get_settings

    env = get_settings()
    return (env,)


@app.cell
def _():
    import os
    from pathlib import Path

    import altair as alt
    import marimo as mo
    import numpy as np
    import pandas as pd
    import polars as pl
    from scipy.stats import pearsonr
    from wholecell.utils.protein_counts import get_simulated_validation_counts

    return (
        Path,
        alt,
        get_simulated_validation_counts,
        mo,
        np,
        os,
        pd,
        pearsonr,
        pl,
    )


@app.cell
def _():
    from viva_api.data.data_service import PARTITION_GROUPS, AnalysisType, SimulationDataServiceFS

    data_service = SimulationDataServiceFS()
    return (
        AnalysisType,
        PARTITION_GROUPS,
        SimulationDataServiceFS,
        data_service,
    )


@app.cell
def _(data_service):
    # wd_root = str(data_service.wd_root)
    sim_data = data_service.sim_data
    validation_data = data_service.validation_data
    mrna_cistron_names = data_service.labels.mrna_cistron_names
    monomer_names = data_service.labels.monomer_names
    monomer_ids = data_service.labels.monomer_ids
    rxn_ids = data_service.labels.rxn_ids
    return (
        monomer_ids,
        monomer_names,
        mrna_cistron_names,
        rxn_ids,
        sim_data,
        validation_data,
    )


@app.cell
def _(Path, mo, np, os, pd):
    def get_pathways(pathway_dir):
        pathway_file = os.path.join(pathway_dir, "pathways.txt")
        pathway_df = pd.read_csv(pathway_file, sep="\t")
        pathway_list = pathway_df["name"].values
        pathway_list = list(np.unique(pathway_list))
        return pathway_list

    analysis_select = mo.ui.dropdown(
        options=["single", "multidaughter", "multigeneration", "multiseed"],
        value="single",
    )

    pathway_dir = str(Path(__file__).parent.parent.parent / "assets" / "app" / "pathways")
    select_pathway = mo.ui.dropdown(
        options=get_pathways(pathway_dir), searchable=True, value="3-dehydroquinate biosynthesis I"
    )

    y_scale = mo.ui.dropdown(options=["linear", "log", "symlog"], value="linear")

    molecule_id_type = mo.ui.radio(options=["Common name", "BioCyc ID"], value="BioCyc ID")
    return (
        analysis_select,
        molecule_id_type,
        pathway_dir,
        select_pathway,
        y_scale,
    )


@app.cell
def _(bulk_override, data_service, mo, molecule_id_type, select_pathway):
    molecule_id_options = data_service.labels.bulk_common_names

    if molecule_id_type.value == "Common name":
        molecule_id_options = data_service.labels.bulk_common_names
    elif molecule_id_type.value == "BioCyc ID":
        molecule_id_options = data_service.labels.bulk_names_unique

    bulk_sp_plot = mo.ui.multiselect(
        options=molecule_id_options,
        value=bulk_override(select_pathway.value),
        max_selections=500,
    )
    return (bulk_sp_plot,)


@app.cell
def _(PARTITION_GROUPS, analysis_select, partitions_display):
    partitions_req = PARTITION_GROUPS[analysis_select.value]
    partitions_select_all = partitions_display()
    partition_selector = []
    for i in range(len(partitions_req)):
        partition_selector.append(str(partitions_req[i]) + ":")
        partition_selector.append(partitions_select_all[partitions_req[i]])
    return (partition_selector,)


@app.cell
def _(env, mo):
    def available_experiments():
        exps = []
        for path in env.simulation_outdir.remote_path.iterdir():
            exp_name = path.parts[-1]
            if path.is_dir() and "analysis" not in exp_name:
                exps.append(exp_name)
        return exps

    exp_select = mo.ui.dropdown(
        options=available_experiments(),
        value="sms_multigeneration",
    )
    return (exp_select,)


@app.cell
def _(exp_select, get_variants, mo):
    variant_select = mo.ui.dropdown(options=get_variants(exp_id=exp_select.value), value="0")
    return (variant_select,)


@app.cell
def _(exp_select, get_seeds, mo, variant_select):
    seed_select = mo.ui.dropdown(options=get_seeds(exp_id=exp_select.value, var_id=variant_select.value), value="5")
    return (seed_select,)


@app.cell
def _(exp_select, get_gens, mo, seed_select, variant_select):
    gen_select = mo.ui.dropdown(
        options=get_gens(
            exp_id=exp_select.value,
            var_id=variant_select.value,
            seed_id=seed_select.value,
        ),
        value="1",
    )
    return (gen_select,)


@app.cell
def _(exp_select, gen_select, get_agents, mo, seed_select, variant_select):
    agent_select = mo.ui.dropdown(
        options=get_agents(
            exp_id=exp_select.value,
            var_id=variant_select.value,
            seed_id=seed_select.value,
            gen_id=gen_select.value,
        ),
        value="0",
    )
    return (agent_select,)


@app.cell
def _(agent_select, exp_select, gen_select, seed_select, variant_select):
    def read_partitions(exp_select, variant_select, seed_select, gen_select, agent_select):
        partitions_selected = {
            "experiment_id": exp_select.value,
            "variant": variant_select.value,
            "lineage_seed": seed_select.value,
            "generation": gen_select.value,
            "agent_id": agent_select.value,
        }
        return partitions_selected

    partitions = read_partitions(exp_select, variant_select, seed_select, gen_select, agent_select)
    return (partitions,)


@app.cell
def _(AnalysisType, analysis_select, data_service, partitions):
    output_loaded = data_service.get_outputs(
        analysis_type=AnalysisType[analysis_select.value.upper()],
        partitions_all=partitions,
        exp_select="sms_multigeneration",
    )
    return (output_loaded,)


@app.cell
def _(mo):
    mo.md(r"""
    <h1 style="font-family: monospace, sans-serif;">
        SMS Data Explorer
    </h1>
    """)
    return


@app.cell
def _(analysis_select, mo):
    mo.hstack(["analysis type:", analysis_select], justify="start")
    return


@app.cell
def _(mo, partition_selector):
    mo.hstack(partition_selector, justify="start")
    return


@app.cell
def _(mo, select_pathway):
    mo.hstack(["pathway:", select_pathway], justify="start")
    return


@app.cell
def _(mo):
    mo.md("""
    <p style="font-family: Arial, sans-serif;">
        </p>

    **Compound Molecule Counts:** The "bulk" store in the vEcoli model tracks
    individual molecule counts of modeled comopunds, namely, transcription units, RNAs,
    proteins, complexes, metabolites and small molecules. In this section, we generate
    time course plots of user selected compounds. If no pathway is selected, you may
    specify compounds to plot from the following menu, using their BioCyc IDs or display names.
    """)
    return


@app.cell
def _(bulk_sp_plot, mo, molecule_id_type, y_scale):
    bulk_select = [
        mo.ui.button(label="""Compound Molecule Counts """),
        "label type:",
        molecule_id_type,
    ]

    if molecule_id_type.value == "Common name":
        bulk_select.append("name:")
        bulk_select.append(bulk_sp_plot)
    elif molecule_id_type.value == "BioCyc ID":
        bulk_select.append("id:")
        bulk_select.append(bulk_sp_plot)

    bulk_select.append("Scale:")
    bulk_select.append(y_scale)

    mo.hstack(bulk_select, justify="start")
    return


@app.cell
def _(
    alt,
    bulk_sp_plot,
    data_service,
    mo,
    molecule_id_type,
    output_loaded,
    y_scale,
):
    dfds_long = data_service.get_bulk_df(output_loaded, molecule_id_type.value, bulk_sp_plot.value)
    mo.ui.altair_chart(
        alt.Chart(dfds_long)
        .mark_line()
        .encode(
            x=alt.X("time:Q", scale=alt.Scale(type="linear"), axis=alt.Axis(tickCount=4)),
            y=alt.Y("counts:Q", scale=alt.Scale(type=y_scale.value)),
            color="Compounds:N",
        )
    )
    return


@app.cell
def _(mo):
    mo.md("""
    <p style="font-family: Arial, sans-serif;">
        </p>

    **mRNA Counts:** In this section, we generate time course plots of selected mRNA cistron counts.
    If no pathway is selected, mRNAs may be specified with gene names or their BioCyc IDs.
    """)
    return


@app.cell
def _(mo):
    rna_label_type = mo.ui.radio(options=["gene name", "BioCyc ID"], value="gene name")

    y_scale_mrna = mo.ui.dropdown(options=["linear", "log", "symlog"], value="linear")

    monomer_label_type = mo.ui.radio(options=["common name", "BioCyc ID"], value="common name")

    y_scale_monomers = mo.ui.dropdown(options=["linear", "log", "symlog"], value="symlog")
    return monomer_label_type, rna_label_type, y_scale_monomers, y_scale_mrna


@app.cell
def _(mo, mrna_select_plot, rna_label_type, y_scale_mrna):
    mrna_select_menu = [
        mo.ui.button(label="mRNA Counts"),
        "label type:",
        rna_label_type,
    ]

    if rna_label_type.value == "gene name":
        mrna_select_menu.append("name:")
        mrna_select_menu.append(mrna_select_plot)
    elif rna_label_type.value == "BioCyc ID":
        mrna_select_menu.append("ID:")
        mrna_select_menu.append(mrna_select_plot)

    mrna_select_menu.append("Scale:")
    mrna_select_menu.append(y_scale_mrna)

    mo.hstack(mrna_select_menu, justify="start")
    return


@app.cell
def _(
    mo,
    mrna_cistron_names,
    mrna_gene_ids,
    mrna_override,
    rna_label_type,
    select_pathway,
):
    if rna_label_type.value == "gene name":
        rna_label_options = mrna_cistron_names
    elif rna_label_type.value == "BioCyc ID":
        rna_label_options = mrna_gene_ids

    mrna_select_plot = mo.ui.multiselect(
        options=rna_label_options,
        value=mrna_override(select_pathway.value),
        max_selections=500,
    )
    return (mrna_select_plot,)


@app.cell
def _(
    mo,
    monomer_ids,
    monomer_label_type,
    monomer_names,
    protein_override,
    select_pathway,
):
    monomer_label_dict = {"common name": monomer_names, "BioCyc ID": monomer_ids}

    monomer_select_plot = mo.ui.multiselect(
        options=monomer_label_dict[monomer_label_type.value],
        value=protein_override(select_pathway.value),
        max_selections=500,
    )
    return (monomer_select_plot,)


@app.cell
def _(
    alt,
    data_service,
    mo,
    mrna_select_plot,
    output_loaded,
    rna_label_type,
    y_scale_mrna,
):
    mrna_dfds_long = data_service.get_mrna_df(output_loaded, rna_label_type.value, mrna_select_plot.value)
    mo.ui.altair_chart(
        alt.Chart(mrna_dfds_long)
        .mark_line()
        .encode(
            x=alt.X("time:Q", scale=alt.Scale(type="linear"), axis=alt.Axis(tickCount=4)),
            y=alt.Y("counts:Q", scale=alt.Scale(type=y_scale_mrna.value)),
            color="Genes:N",
        )
    )
    return (mrna_dfds_long,)


@app.cell
def _(mo):
    mo.md("""
    <p style="font-family: Arial, sans-serif;">
        </p>
    **Protein Monomer Counts:** This time course plot visualizes the protein content of the
    simulation output in terms of monomer counts. Monomers to plot can be specified with their
    BioCyc IDs or display names.
    """)
    return


@app.cell
def _(mo, monomer_label_type, monomer_select_plot, y_scale_monomers):
    monomer_menu_text = {"common name": "name: ", "BioCyc ID": "ID: "}
    monomer_select_menu = [
        mo.ui.button(label="protein monomer counts"),
        "label type:",
        monomer_label_type,
        monomer_menu_text[monomer_label_type.value],
        monomer_select_plot,
        "scale: ",
        y_scale_monomers,
    ]

    mo.hstack(monomer_select_menu, justify="start")
    return


@app.cell
def _(
    alt,
    data_service,
    mo,
    monomer_label_type,
    monomer_select_plot,
    output_loaded,
    y_scale_monomers,
):
    monomer_dfds_long = data_service.get_monomers_df(output_loaded, monomer_label_type.value, monomer_select_plot.value)

    mo.ui.altair_chart(
        alt.Chart(monomer_dfds_long)
        .mark_line()
        .encode(
            x=alt.X("time:Q", scale=alt.Scale(type="linear"), axis=alt.Axis(tickCount=4)),
            y=alt.Y("counts:Q", scale=alt.Scale(type=y_scale_monomers.value)),
            color="protein names:N",
        )
    )
    return


@app.cell
def _(mo):
    mo.md("""
    <p style="font-family: Arial, sans-serif;">
        </p>

    **Metabolic Reaction Fluxes:** In this plot, we visualize time course of metabolic reaction fluxes.
    Individual reactions can be selected using their BioCyc IDs
    """)
    return


@app.cell
def _(mo, rxn_ids, rxn_override, select_pathway):
    select_rxns = mo.ui.multiselect(options=rxn_ids, value=rxn_override(select_pathway.value), max_selections=500)
    y_scale_rxns = mo.ui.dropdown(options=["linear", "log", "symlog"], value="symlog")
    mo.hstack(
        [
            mo.ui.button(label="Reaction Fluxes"),
            "Reaction ID(s):",
            select_rxns,
            "scale:",
            y_scale_rxns,
        ],
        justify="start",
    )
    return select_rxns, y_scale_rxns


@app.cell
def _(alt, data_service, mo, output_loaded, select_rxns, y_scale_rxns):
    rxns_dfds_long = data_service.get_rxns_df(output_loaded, select_rxns.value)
    mo.ui.altair_chart(
        alt.Chart(rxns_dfds_long)
        .mark_line()
        .encode(
            x=alt.X("time:Q", scale=alt.Scale(type="linear"), axis=alt.Axis(tickCount=4)),
            y=alt.Y("flux:Q", scale=alt.Scale(type=y_scale_rxns.value)),
            color="reaction_id:N",
        )
    )
    return (rxns_dfds_long,)


@app.cell
def _(
    AnalysisType,
    analysis_select,
    data_service,
    exp_select,
    get_simulated_validation_counts,
    get_val_ids,
    partitions,
    sim_data,
    validation_data,
):
    monomer_counts = data_service.get_monomer_counts(
        exp_select=exp_select.value,
        analysis_type=AnalysisType[analysis_select.value.upper()],
        partitions_all=partitions,
    )
    sim_monomer_ids = sim_data.process.translation.monomer_data["id"]
    wisniewski_ids = validation_data.protein.wisniewski2014Data["monomerId"]
    schmidt_ids = validation_data.protein.schmidt2015Data["monomerId"]
    wisniewski_counts = validation_data.protein.wisniewski2014Data["avgCounts"]
    schmidt_counts = validation_data.protein.schmidt2015Data["glucoseCounts"]
    sim_wisniewski_counts, val_wisniewski_counts = get_simulated_validation_counts(
        wisniewski_counts, monomer_counts, wisniewski_ids, sim_monomer_ids
    )
    sim_schmidt_counts, val_schmidt_counts = get_simulated_validation_counts(
        schmidt_counts, monomer_counts, schmidt_ids, sim_monomer_ids
    )
    schmidt_val_ids = get_val_ids(schmidt_ids, sim_monomer_ids)
    wisniewski_val_ids = get_val_ids(wisniewski_ids, sim_monomer_ids)

    val_options = {
        "Schmidt 2015": {
            "id": schmidt_val_ids,
            "data": val_schmidt_counts,
            "sim": sim_schmidt_counts,
        },
        "Wisniewski 2014": {
            "id": wisniewski_val_ids,
            "data": val_wisniewski_counts,
            "sim": sim_wisniewski_counts,
        },
    }
    return (val_options,)


@app.cell
def _(mo):
    val_dataset_select = mo.ui.dropdown(options=["Schmidt 2015", "Wisniewski 2014"], value="Schmidt 2015")
    val_label_type = mo.ui.dropdown(options=["Common Name", "BioCyc ID"], value="Common Name")
    return val_dataset_select, val_label_type


@app.cell
def _(
    mo,
    protein_val_override,
    select_pathway,
    val_dataset_select,
    val_options,
):
    val_id_select = mo.ui.multiselect(
        options=val_options[val_dataset_select.value]["id"],
        value=protein_val_override(select_pathway.value),
    )
    return (val_id_select,)


@app.cell
def _(data_service, sim_data, val_label_type):
    def get_val_ids(data_ids, sim_ids):
        sim_ids_lst = sim_ids.tolist()
        data_ids_lst = data_ids.tolist()
        overlapping_ids_set = set(sim_ids_lst) & set(data_ids_lst)
        val_ids = list(overlapping_ids_set)
        val_ids = [id[:-3] for id in val_ids]
        val_ids_mapping = {
            "Common Name": data_service.get_common_names(val_ids, sim_data),
            "BioCyc ID": val_ids,
        }
        val_ids_final = val_ids_mapping[val_label_type.value]
        return val_ids_final

    return (get_val_ids,)


@app.cell
def _(mo):
    mo.md("""
    <p style="font-family: Arial, sans-serif;">
        </p>
    **Protein Count Validation:** This is a scatter plot comparing simulated average
    protein counts to experimental proteomics datasets. This is applicable to proteins
    overlapping the modeled proteins and either of the validation datasets.
    You may choose to visualize all available proteins or pathway specific proteins.
    Alternatively, the attached drop down menu can be used to select proteins using
    their BioCyc IDs or display names.
    """)
    return


@app.cell
def _(mo, val_dataset_select, val_id_select, val_label_type):
    val_menu_text = {"Common Name": "Name: ", "BioCyc ID": "ID: "}
    val_select_menu = [
        mo.ui.button(label="Protein Count Validation"),
        "Validation Dataset: ",
        val_dataset_select,
        "Label Type: ",
        val_label_type,
        val_menu_text[val_label_type.value],
        val_id_select,
    ]

    mo.hstack(val_select_menu, justify="start")
    return


@app.cell
def _(alt, mo, np, pearsonr, pl, val_id_select, val_options):
    def val_chart(dataset_name):
        data_val = val_options[dataset_name]["data"]
        data_sim = val_options[dataset_name]["sim"]
        data_idxs = [val_options[dataset_name]["id"].index(name) for name in val_id_select.value]
        data_val_filtered = data_val[data_idxs]
        data_sim_filtered = data_sim[data_idxs]

        chart = (
            alt.Chart(
                pl.DataFrame({
                    dataset_name: np.log10(data_val_filtered + 1),
                    "sim": np.log10(data_sim_filtered + 1),
                    "protein": val_id_select.value,
                })
            )
            .mark_point()
            .encode(
                x=alt.X(dataset_name, title=f"log10({dataset_name} Counts + 1)"),
                y=alt.Y("sim", title="log10(Simulation Average Counts + 1)"),
                tooltip=["protein:N"],
            )
            .properties(
                title="Pearson r: %0.2f" % pearsonr(np.log10(data_sim_filtered + 1), np.log10(data_val_filtered + 1))[0]
            )
        )

        max_val = max(
            np.log10(val_options["Schmidt 2015"]["data"] + 1).max(),
            np.log10(val_options["Wisniewski 2014"]["data"] + 1).max(),
            np.log10(val_options["Schmidt 2015"]["sim"] + 1).max(),
            np.log10(val_options["Wisniewski 2014"]["sim"] + 1).max(),
        )
        parity = (
            alt.Chart(pl.DataFrame({"x": np.arange(max_val)}))
            .mark_line()
            .encode(x="x", y="x", color=alt.value("red"), strokeDash=alt.value([5, 5]))
        )

        chart_final = chart + parity

        return mo.ui.altair_chart(chart_final)

    return (val_chart,)


@app.cell
def _(val_chart, val_dataset_select):
    val_chart(val_dataset_select.value)
    return


@app.cell
def _(data_service, exp_select, os):
    def get_variants(exp_id, outdir=str(data_service.outputs_dir.remote_path)):
        try:
            vars_ls = os.listdir(
                os.path.join(
                    outdir,
                    exp_select.value,
                    "history",
                    f"experiment_id={exp_select.value}",
                )
            )

            variant_folders = [folder for folder in vars_ls if not folder.startswith(".")]

            variants = [var.split("variant=")[1] for var in variant_folders]

        except (FileNotFoundError, TypeError):
            variants = ["N/A"]

        return variants

    def get_seeds(exp_id, var_id, outdir=str(data_service.outputs_dir.remote_path)):
        try:
            seeds_ls = os.listdir(
                os.path.join(
                    outdir,
                    exp_select.value,
                    "history",
                    f"experiment_id={exp_select.value}",
                    f"variant={var_id}",
                )
            )
            seed_folders = [folder for folder in seeds_ls if not folder.startswith(".")]

            seeds = [seed.split("lineage_seed=")[1] for seed in seed_folders]
        except (FileNotFoundError, TypeError):
            seeds = ["N/A"]

        return seeds

    def get_gens(exp_id, var_id, seed_id, outdir=str(data_service.outputs_dir.remote_path)):
        try:
            gens_ls = os.listdir(
                os.path.join(
                    outdir,
                    exp_select.value,
                    "history",
                    f"experiment_id={exp_select.value}",
                    f"variant={var_id}",
                    f"lineage_seed={seed_id}",
                )
            )

            gen_folders = [folder for folder in gens_ls if not folder.startswith(".")]

            gens = [gen.split("generation=")[1] for gen in gen_folders]
        except (FileNotFoundError, TypeError):
            gens = ["N/A"]

        return gens

    def get_agents(exp_id, var_id, seed_id, gen_id, outdir=str(data_service.outputs_dir.remote_path)):
        try:
            agents_ls = os.listdir(
                os.path.join(
                    outdir,
                    exp_select.value,
                    "history",
                    f"experiment_id={exp_select.value}",
                    f"variant={var_id}",
                    f"lineage_seed={seed_id}",
                    f"generation={gen_id}",
                )
            )

            agent_folders = [folder for folder in agents_ls if not folder.startswith(".")]
            agents = [agent.split("agent_id=")[1] for agent in agent_folders]
        except (FileNotFoundError, TypeError):
            agents = ["N/A"]

        return agents

    return get_agents, get_gens, get_seeds, get_variants


@app.cell
def _(
    SimulationDataServiceFS,
    agent_select,
    data_service,
    exp_select,
    gen_select,
    molecule_id_type,
    monomer_label_type,
    mrna_cistron_names,
    np,
    os,
    pathway_dir,
    pd,
    rna_label_type,
    seed_select,
    val_dataset_select,
    val_options,
    variant_select,
):
    def partitions_display():
        partitions_list = {
            "experiment_id": exp_select,
            "variant": variant_select,
            "lineage_seed": seed_select,
            "generation": gen_select,
            "agent_id": agent_select,
        }

        return partitions_list

    def get_presets(preset_dir):
        preset_files = os.listdir(preset_dir)
        presets_list = [file.split(".")[0] for file in preset_files]

        return presets_list

    def read_columns(st_column):
        values = []
        for item in st_column:
            items_actual = str(item).split(" // ")
            for item_actual in items_actual:
                values.append(item_actual)
        return values

    def read_presets(pathway_name):
        preset_dict = {}
        if isinstance(pathway_name, str):
            preset_table = pd.read_csv(os.path.join(pathway_dir, "pathways.txt"), header=0, sep="\t")
            pathway_df = preset_table[preset_table["name"] == pathway_name]

            preset_dict["reactions"] = read_columns(pathway_df["reactions"])
            preset_dict["genes"] = read_columns(pathway_df["genes"])
            preset_dict["compounds"] = read_columns(pathway_df["compounds"])

        return preset_dict

    def preset_override(preset_name, data_service: SimulationDataServiceFS):
        # rxn ids
        # mrna_gene_ids
        # bulk_names_unique
        # monomer ids
        # monomer names
        preset_dict = read_presets(preset_name)

        rxn_ids = data_service.labels.rxn_ids
        mrna_gene_ids = data_service.labels.mrna_gene_ids
        bulk_names_unique = data_service.labels.bulk_names_unique
        monomer_ids = data_service.labels.monomer_ids
        bulk_common_names = data_service.labels.bulk_common_names
        monomer_names = data_service.labels.monomer_names

        preset_final = {}

        if len(preset_dict) > 0:
            preset_final["reactions"] = np.array(preset_dict["reactions"])[
                np.isin(preset_dict["reactions"], rxn_ids)
            ].tolist()

            preset_final["genes"] = np.array(preset_dict["genes"])[
                np.isin(preset_dict["genes"], mrna_gene_ids)
            ].tolist()

            preset_final["genes"] = np.unique(preset_final["genes"]).tolist()

            if rna_label_type.value == "gene name":
                preset_gene_names = []
                for gene_id in preset_final["genes"]:
                    preset_gene_names.append(mrna_cistron_names[mrna_gene_ids.index(gene_id)])
                preset_final["genes"] = preset_gene_names

            preset_final["compounds"] = np.array(preset_dict["compounds"])[
                np.isin(preset_dict["compounds"], bulk_names_unique)
            ].tolist()

            preset_final["compounds"] = np.unique(preset_final["compounds"]).tolist()

            preset_final["proteins"] = list(
                np.array(preset_final["compounds"])[np.isin(preset_final["compounds"], monomer_ids)]
            )

            if molecule_id_type.value == "Common name":
                preset_compound_names = []
                for name in preset_final["compounds"]:
                    preset_compound_names.append(bulk_common_names[bulk_names_unique.index(name)])
                preset_final["compounds"] = preset_compound_names

            if monomer_label_type.value == "common name":
                preset_protein_names = []
                for name in preset_final["proteins"]:
                    preset_protein_names.append(monomer_names[monomer_ids.index(name)])
                preset_final["proteins"] = preset_protein_names

        return preset_final

    def bulk_override(preset_name):
        preset_dict = preset_override(preset_name, data_service)
        bulk_list = preset_dict.get("compounds")
        return bulk_list

    def rxn_override(preset_name):
        preset_dict = preset_override(preset_name, data_service)
        rxn_list = preset_dict.get("reactions")
        return rxn_list

    def mrna_override(preset_name):
        preset_dict = preset_override(preset_name, data_service)
        mrna_list = preset_dict.get("genes")
        return mrna_list

    def protein_override(preset_name):
        preset_dict = preset_override(preset_name, data_service)
        protein_list = preset_dict.get("proteins")
        return protein_list

    def protein_val_override(preset_name):
        protein_list = protein_override(preset_name)
        dataset_name = val_dataset_select.value
        protein_ids_val = val_options[dataset_name]["id"]
        protein_val = list(np.array(protein_list)[np.isin(protein_list, protein_ids_val)])
        return protein_val

    return (
        bulk_override,
        mrna_override,
        partitions_display,
        protein_override,
        protein_val_override,
        rxn_override,
    )


@app.cell
def _(mo, rxns_dfds_long):
    def download_fluxomics():
        data = rxns_dfds_long.to_json().encode("utf-8")
        return data

    flux_download = mo.download(
        data=download_fluxomics, filename="fluxomics.json", mimetype="application/json", label="Export Fluxomics Data"
    )
    return (flux_download,)


@app.cell
def _(mo, mrna_dfds_long):
    def download_transcriptomics() -> None:
        data = mrna_dfds_long.to_json()
        return data

    trans_download = mo.download(
        data=download_transcriptomics,
        filename="transcriptomics.json",
        mimetype="application/json",
        label="Export Transcriptomics Data",
    )
    return (trans_download,)


@app.cell
def _(dfds_dfds_long, mo):
    def download_proteomics() -> None:
        data = dfds_dfds_long.to_json()
        return data

    prot_download = mo.download(
        data=download_proteomics,
        filename="proteomics.json",
        mimetype="application/json",
        label="Export Proteomics Data",
    )
    return (prot_download,)


@app.cell
def _(flux_download, mo, prot_download, trans_download):
    mo.hstack([prot_download, trans_download, flux_download], justify="space-around")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
