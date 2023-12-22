.. _introduction:

************
Introduction
************

Jobflow-remote is a free, open-source library serving as a manager for the execution
of `jobflow <https://materialsproject.github.io/jobflow/>`_ workflows. While jobflow is
not bound to be executed with a specific manager and some adapter has already been
developed (*e.g.* `Fireworks <https://materialsproject.github.io/fireworks/>`_),
jobflow-remote has been designed to take full advantage of and adapt to jobflow's
functionalities and interact with the typical high performance computing center
accessible by researchers.

Jobflow's Jobs functions are executed directly on the computing resources, however,
differently from `Fireworks <https://materialsproject.github.io/fireworks/>`_, all the
interactions with the output Stores are handled by a daemon process, called ``runner``.
This allows to bypass the problem of computing center not having direct access to the
user's database.
Given the relatively small requirements, this gives the freedom to run jobflow-remote's
daemon

* on a workstation that has access to the computing resource
* or directly on the front-end of the cluster

Following a short list of basic features

* Fully compatible with `jobflow <https://materialsproject.github.io/jobflow/>`_
* Data storage based on mongo-like `maggma <https://materialsproject.github.io/maggma/>`_ Stores.
* Simple single file configuration as a starting point. Can scale to handle different projects with different configurations
* Fully configurable submission options
* Management through python API and command line interface
* Parallelized daemon execution
* Limit number of jobs submitted per worker
* Batch submission (experimental)
