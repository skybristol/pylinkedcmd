=======================================
Python for Linked Corporate Master Data
=======================================


.. image:: https://img.shields.io/pypi/v/pylinkedcmd.svg
        :target: https://pypi.python.org/pypi/pylinkedcmd

.. image:: https://img.shields.io/travis/skybristol/pylinkedcmd.svg
        :target: https://travis-ci.com/skybristol/pylinkedcmd

.. image:: https://readthedocs.org/projects/pylinkedcmd/badge/?version=latest
        :target: https://pylinkedcmd.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. image:: https://colab.research.google.com/assets/colab-badge.svg
        :target: https://colab.research.google.com/github/skybristol/pylinkedcmd/tree/dev/examples
        :alt: Run Examples on Google Colab

Python for Linked Corporate Master Data provides tools for making corporate master data from the USGS more linked and more open. This package is being developed to meet specific needs in the USGS for working with our various public facing data systems in ways that expose that information for new uses, run transformations that expose linked data, or solve specific usability problems. However, we hope the design patterns we employ can prove useful to other groups dealing with similar challenges, and we will make every attempt to always look toward open standards and community best practices for our methodology.


* Free software: Unlicense
* Documentation: https://pylinkedcmd.readthedocs.io.

Provisional Software Statement
------------------------------
Under USGS Software Release Policy, the software codes here are considered preliminary, not released officially, and posted to this repo for informal sharing among colleagues.

This software is preliminary or provisional and is subject to revision. It is being provided to meet the need for timely best science. The software has not received final approval by the U.S. Geological Survey (USGS). No warranty, expressed or implied, is made by the USGS or the U.S. Government as to the functionality of the software and related material nor shall the fact of release constitute any such warranty. The software is provided on the condition that neither the USGS nor the U.S. Government shall be held liable for any damages resulting from the authorized or unauthorized use of the software.

Features
--------

Wikidata
~~~~~~~~
One of the things we are experimenting with here is a possible role for Wikidata as a neutral, third-party broker for links and information about USGS corporate assets that can live beyond and outside our own necessary constraints. There is already a fair bit of information about USGS and our assets (people, facilities, products, etc.) that has found its way into Wikidata through a variety of pathways. We can take advantage of what's there and add to it judiciously within the governance model of the Wikiverse.

USGS Web
~~~~~~~~
In a number of cases, we have to apply some (hopefully) temporary measures to work with online information that is not particularly friendly to software code or that does not comply with linked data methods in terms of structure and/or semantics. We've written a handful of functions for dealing with these dynamics.

Notebooks
~~~~~~~~~
The examples are written and shared as Jupyter Notebooks in Python. These are built to document our thought processes and show examples of how we are working with this code.

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
