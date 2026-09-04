Atlantis
========

.. image:: _static/wholecellecoli.png
   :width: 200px
   :align: right

**Atlantis** is a platform for designing, running, and analyzing biological
simulations --- from whole-cell *E. coli* models to composable
`process-bigraph <https://github.com/vivarium-collective/process-bigraph>`_
workflows using COPASI, Tellurium, v2ecoli, and more.

You interact with Atlantis through one of three client applications --- each
exposes the same end-to-end workflow (build a simulator, run a simulation,
download results) in a different format:

.. list-table::
   :widths: 15 35 50

   * - **CLI**
     - ``uv run atlantis``
     - Command-line interface (fastest for scripting)
   * - **TUI**
     - ``uv run atlantis tui``
     - Interactive terminal UI with sidebar navigation
   * - **Web GUI**
     - ``uv run atlantis gui``
     - Marimo notebook UI (opens in browser)

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   getting-started/installation
   getting-started/quickstart

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   guides/end-to-end-workflow
   guides/running-a-campaign
   guides/composite-dispatch
   guides/choosing-a-client
   guides/cli-reference
   guides/compose
   guides/compose-tutorial
   guides/biomodels
   guides/analysis-filtering
   guides/simulation-filtering

.. toctree::
   :maxdepth: 2
   :caption: Reference

   reference/configuration
   reference/run-times

.. toctree::
   :maxdepth: 2
   :caption: Architecture (Internal)

   architecture/overview
   guides/aws-s3-setup
   guides/qumulo-setup
   architecture/aws-batch
   architecture/build-pipeline
   architecture/analysis-results-design
   architecture/pbg-native-composite-dispatch

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api/modules
