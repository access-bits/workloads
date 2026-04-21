# Makefile-based Benchmarking Infrastructure
# Scott Beamer, 2017



# Generate Input Graphs ------------------------------------------------#
#-----------------------------------------------------------------------#

# Allow overriding directories via environment variables
GRAPH_DIR ?= benchmark/graphs
OUTPUT_DIR ?= benchmark/out
RAW_GRAPH_DIR = $(GRAPH_DIR)/raw

GRAPHS = twitter web road kron urand
ALL_GRAPHS =\
	$(addsuffix .sg, $(GRAPHS)) \
	$(addsuffix U.sg, $(GRAPHS))
ALL_GRAPHS_WITH_PATHS = $(addprefix $(GRAPH_DIR)/, $(ALL_GRAPHS))

$(RAW_GRAPH_DIR):
	mkdir -p $@

.PHONY: bench-graphs
bench-graphs: $(RAW_GRAPH_DIR) $(ALL_GRAPHS_WITH_PATHS)


# Real-world

TWITTER_BASE_URL := https://github.com/ANLAB-KAIST/traces/releases/download/twitter_rv.net/twitter_rv.net

# Download all Twitter files in parallel (using stamp file to track completion)
$(RAW_GRAPH_DIR)/.twitter_download_complete: | $(RAW_GRAPH_DIR)
	@echo "Downloading Twitter files in parallel..."
	@wget -P $(RAW_GRAPH_DIR) $(TWITTER_BASE_URL).00.gz & \
	wget -P $(RAW_GRAPH_DIR) $(TWITTER_BASE_URL).01.gz & \
	wget -P $(RAW_GRAPH_DIR) $(TWITTER_BASE_URL).02.gz & \
	wget -P $(RAW_GRAPH_DIR) $(TWITTER_BASE_URL).03.gz & \
	wait
	@echo "All Twitter files downloaded"
	@touch $@

$(RAW_GRAPH_DIR)/twitter_rv.net.00.gz: $(RAW_GRAPH_DIR)/.twitter_download_complete
$(RAW_GRAPH_DIR)/twitter_rv.net.01.gz: $(RAW_GRAPH_DIR)/.twitter_download_complete
$(RAW_GRAPH_DIR)/twitter_rv.net.02.gz: $(RAW_GRAPH_DIR)/.twitter_download_complete
$(RAW_GRAPH_DIR)/twitter_rv.net.03.gz: $(RAW_GRAPH_DIR)/.twitter_download_complete

$(RAW_GRAPH_DIR)/twitter_rv.net: $(RAW_GRAPH_DIR)/twitter_rv.net.00.gz $(RAW_GRAPH_DIR)/twitter_rv.net.01.gz $(RAW_GRAPH_DIR)/twitter_rv.net.02.gz $(RAW_GRAPH_DIR)/twitter_rv.net.03.gz
	@echo "Decompressing Twitter files in parallel..."
	gunzip -c $(RAW_GRAPH_DIR)/twitter_rv.net.00.gz > $(RAW_GRAPH_DIR)/twitter_rv.net.00 & \
	gunzip -c $(RAW_GRAPH_DIR)/twitter_rv.net.01.gz > $(RAW_GRAPH_DIR)/twitter_rv.net.01 & \
	gunzip -c $(RAW_GRAPH_DIR)/twitter_rv.net.02.gz > $(RAW_GRAPH_DIR)/twitter_rv.net.02 & \
	gunzip -c $(RAW_GRAPH_DIR)/twitter_rv.net.03.gz > $(RAW_GRAPH_DIR)/twitter_rv.net.03 & \
	wait
	@echo "Concatenating files..."
	cat $(RAW_GRAPH_DIR)/twitter_rv.net.00 $(RAW_GRAPH_DIR)/twitter_rv.net.01 $(RAW_GRAPH_DIR)/twitter_rv.net.02 $(RAW_GRAPH_DIR)/twitter_rv.net.03 > $@
	rm -f $(RAW_GRAPH_DIR)/twitter_rv.net.00 $(RAW_GRAPH_DIR)/twitter_rv.net.01 $(RAW_GRAPH_DIR)/twitter_rv.net.02 $(RAW_GRAPH_DIR)/twitter_rv.net.03
	touch $@

$(RAW_GRAPH_DIR)/twitter.el: $(RAW_GRAPH_DIR)/twitter_rv.net
	rm -f $@
	ln -s twitter_rv.net $@

$(GRAPH_DIR)/twitter.sg: $(RAW_GRAPH_DIR)/twitter.el converter
	./converter -f $< -b $@

$(GRAPH_DIR)/twitterU.sg: $(RAW_GRAPH_DIR)/twitter.el converter
	./converter -sf $< -b $@

ROAD_URL = http://www.dis.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.USA.gr.gz
$(RAW_GRAPH_DIR)/USA-road-d.USA.gr.gz:
	wget -P $(RAW_GRAPH_DIR) $(ROAD_URL)

$(RAW_GRAPH_DIR)/USA-road-d.USA.gr: $(RAW_GRAPH_DIR)/USA-road-d.USA.gr.gz
	cd $(RAW_GRAPH_DIR)
	gunzip < $< > $@

$(GRAPH_DIR)/road.sg: $(RAW_GRAPH_DIR)/USA-road-d.USA.gr converter
	./converter -f $< -b $@

$(GRAPH_DIR)/roadU.sg: $(RAW_GRAPH_DIR)/USA-road-d.USA.gr converter
	./converter -sf $< -b $@

WEB_URL = https://sparse.tamu.edu/MM/LAW/sk-2005.tar.gz
$(RAW_GRAPH_DIR)/sk-2005.tar.gz:
	wget -P $(RAW_GRAPH_DIR) $(WEB_URL)

$(RAW_GRAPH_DIR)/sk-2005/sk-2005.mtx: $(RAW_GRAPH_DIR)/sk-2005.tar.gz
	tar -zxvf $< -C $(RAW_GRAPH_DIR)
	touch $@

$(GRAPH_DIR)/web.sg: $(RAW_GRAPH_DIR)/sk-2005/sk-2005.mtx converter
	./converter -f $< -b $@

$(GRAPH_DIR)/webU.sg: $(RAW_GRAPH_DIR)/sk-2005/sk-2005.mtx converter
	./converter -sf $< -b $@


# Synthetic

# Prevent Make from deleting intermediate files (graphs and output files)
.SECONDARY:

# Generic pattern rule for kron graphs of any size
# Usage: make benchmark/graphs/kron<n>.sg where <n> is any integer
$(GRAPH_DIR)/kron%.sg: converter
	./converter -g$* -k16 -b $@

$(GRAPH_DIR)/kron%U.sg: $(GRAPH_DIR)/kron%.sg converter
	rm -f $@
	ln -s $(notdir $<) $@

# Backward compatibility - keep old kron shortcuts
KRON_ARGS = -g27 -k16
$(GRAPH_DIR)/kron.sg: converter
	./converter $(KRON_ARGS) -b $@

$(GRAPH_DIR)/kronU.sg: $(GRAPH_DIR)/kron.sg converter
	rm -f $@
	ln -s kron.sg $@


URAND_ARGS = -u27 -k16
$(GRAPH_DIR)/urand.sg: converter
	./converter $(URAND_ARGS) -b $@

$(GRAPH_DIR)/urandU.sg: $(GRAPH_DIR)/urand.sg converter
	rm -f $@
	ln -s urand.sg $@



# Benchmark Execution --------------------------------------------------#
#-----------------------------------------------------------------------#

$(OUTPUT_DIR):
	mkdir -p $@

# Ordered to reuse input graphs to increase OS file cache hit probability
BENCH_ORDER = pr-twitter pr-web pr-road pr-kron pr-urand

OUTPUT_FILES = $(addsuffix .out, $(addprefix $(OUTPUT_DIR)/, $(BENCH_ORDER)))

.PHONY: pagerank-all
pagerank-all: BENCH_ORDER = pr-twitter pr-web pr-road pr-kron pr-urand
pagerank-all: $(OUTPUT_DIR) $(OUTPUT_FILES)

$(OUTPUT_DIR)/pr-%.out: $(GRAPH_DIR)/%.sg pr
	./pr -f $< -i1000 -t1e-4 -n32 > /dev/null
	@touch $@

# Generic pattern rules for pagerank on any graph
# Usage: make pagerank-<graphtype><n> where graphtype is twitter, web, road, kron, or urand
.PHONY: pagerank-twitter%
pagerank-twitter%: $(OUTPUT_DIR)/pr-twitter%.out
	@echo "Completed pagerank for twitter$*"

.PHONY: pagerank-web%
pagerank-web%: $(OUTPUT_DIR)/pr-web%.out
	@echo "Completed pagerank for web$*"

.PHONY: pagerank-road%
pagerank-road%: $(OUTPUT_DIR)/pr-road%.out
	@echo "Completed pagerank for road$*"

.PHONY: pagerank-kron%
pagerank-kron%: $(OUTPUT_DIR)/pr-kron%.out
	@echo "Completed pagerank for kron$*"

.PHONY: pagerank-urand%
pagerank-urand%: $(OUTPUT_DIR)/pr-urand%.out
	@echo "Completed pagerank for urand$*"


CXX_FLAGS += -std=c++11 -O3 -Wall
PAR_FLAG = -fopenmp

ifneq (,$(findstring icpc,$(CXX)))
	PAR_FLAG = -openmp
endif

ifneq (,$(findstring sunCC,$(CXX)))
	CXX_FLAGS = -std=c++11 -xO3 -m64 -xtarget=native
	PAR_FLAG = -xopenmp
endif

ifneq ($(SERIAL), 1)
	CXX_FLAGS += $(PAR_FLAG)
endif

KERNELS = pr pr_spmv
SUITE = $(KERNELS) converter

.PHONY: all
all: $(SUITE)

$(SUITE): % : src/%.cc src/*.h
	$(CXX) $(CXX_FLAGS) $< -o $@

.PHONY: clean 
clean: 
	rm -f $(SUITE) test/out/*                                
