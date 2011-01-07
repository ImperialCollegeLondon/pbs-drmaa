# $Id: Makefile.am 2623 2009-08-05 12:05:49Z mmamonski $
# 
# FedStage DRMAA for PBS Pro
# Copyright (C) 2006-2009  FedStage Systems
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

docdir=$(datadir)/doc
pkgdocdir=$(docdir)/@PACKAGE@-@VERSION@

plaintext_doc = \
 $(top_srcdir)/README \
 $(top_srcdir)/AUTHORS \
 $(top_srcdir)/COPYING
generated_doc = \
 readme.html \
 readme.pdf \
 stylesheet.css \
 rst.css

EXTRA_DIST = $(generated_doc) readme.tex
pkgdoc_DATA = $(plaintext_doc) $(generated_doc)

DOCUTILSFLAGS = \
 -i utf-8 \
 -o utf-8 \
 --language=en \
 --file-insertion-enabled \
 --no-generator \
 --tab-width=4
RST2HTMLFLAGS = $(DOCUTILSFLAGS) \
 --stylesheet=stylesheet.css \
 --link-stylesheet
RST2LATEXFLAGS = $(DOCUTILSFLAGS) \
 --documentclass=article \
 --documentoptions=a4paper,10pt \
 --font-encoding=OT4 \
 --graphicx-option=pdftex \
 --use-latex-footnotes \
 --footnote-references=superscript \
 --use-latex-citations \
 --use-latex-toc

readme.html: $(top_srcdir)/README
	$(RST2HTML) $(RST2HTMLFLAGS) $< $@
readme.tex: $(top_srcdir)/README
	$(RST2LATEX) $(RST2LATEXFLAGS) $< $@
.tex.pdf:
	pdflatex $< && pdflatex $<

clean-local:
	-rm -rf *.log *.aux *.toc *.out

MAINTAINERCLEANFILES = readme.html readme.tex readme.pdf drmaa.pdf

