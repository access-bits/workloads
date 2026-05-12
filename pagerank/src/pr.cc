// Copyright (c) 2015, The Regents of the University of California (Regents)
// See LICENSE.txt for license details

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <vector>

#include "benchmark.h"
#include "builder.h"
#include "command_line.h"
#include "graph.h"
#include "pvector.h"

/*
GAP Benchmark Suite
Kernel: PageRank (PR)
Author: Scott Beamer

Will return pagerank scores for all vertices once total change < epsilon

This PR implementation uses the traditional iterative approach. It performs
updates in the pull direction to remove the need for atomics, and it allows
new values to be immediately visible (like Gauss-Seidel method). The prior PR
implementation is still available in src/pr_spmv.cc.
*/


using namespace std;

typedef float ScoreT;
const float kDamp = 0.85;
#define PAGE_SIZE 8192  // 4 KB page size * 2 for safety margin

// Magic markers for memory trace instrumentation
volatile int* iter_start_marker = nullptr;
volatile int* iter_end_marker = nullptr;
volatile int* algo_end_marker = nullptr;


pvector<ScoreT> PageRankPullGS(const Graph &g, int max_iters, double epsilon=0,
                               bool logging_enabled = false) {
  const ScoreT init_score = 1.0f / g.num_nodes();
  const ScoreT base_score = (1.0f - kDamp) / g.num_nodes();
  pvector<ScoreT> scores(g.num_nodes(), init_score);
  pvector<ScoreT> outgoing_contrib(g.num_nodes());
  #pragma omp parallel for
  for (NodeID n=0; n < g.num_nodes(); n++)
    outgoing_contrib[n] = init_score / g.out_degree(n);
  
  // Mark initialization complete
  *iter_start_marker = -1;
  
  for (int iter=0; iter < max_iters; iter++) {
    // Mark iteration start
    *iter_start_marker = iter;
    double error = 0;
    #pragma omp parallel for reduction(+ : error) schedule(dynamic, 16384)
    for (NodeID u=0; u < g.num_nodes(); u++) {
      ScoreT incoming_total = 0;
      for (NodeID v : g.in_neigh(u))
        incoming_total += outgoing_contrib[v];
      ScoreT old_score = scores[u];
      scores[u] = base_score + kDamp * incoming_total;
      error += fabs(scores[u] - old_score);
      outgoing_contrib[u] = scores[u] / g.out_degree(u);
    }
    if (logging_enabled)
      PrintStep(iter, error);
    
    // Mark iteration end
    *iter_end_marker = iter;
    
    if (error < epsilon)
      break;
  }
  
  // Mark algorithm end
  *algo_end_marker = max_iters;
  
  return scores;
}


void PrintTopScores(const Graph &g, const pvector<ScoreT> &scores) {
  vector<pair<NodeID, ScoreT>> score_pairs(g.num_nodes());
  for (NodeID n=0; n < g.num_nodes(); n++) {
    score_pairs[n] = make_pair(n, scores[n]);
  }
  int k = 5;
  vector<pair<ScoreT, NodeID>> top_k = TopK(score_pairs, k);
  for (auto kvp : top_k)
    cout << kvp.second << ":" << kvp.first << endl;
}


// Verifies by asserting a single serial iteration in push direction has
//   error < target_error
bool PRVerifier(const Graph &g, const pvector<ScoreT> &scores,
                        double target_error) {
  const ScoreT base_score = (1.0f - kDamp) / g.num_nodes();
  pvector<ScoreT> incoming_sums(g.num_nodes(), 0);
  double error = 0;
  for (NodeID u : g.vertices()) {
    ScoreT outgoing_contrib = scores[u] / g.out_degree(u);
    for (NodeID v : g.out_neigh(u))
      incoming_sums[v] += outgoing_contrib;
  }
  for (NodeID n : g.vertices()) {
    error += fabs(base_score + kDamp * incoming_sums[n] - scores[n]);
    incoming_sums[n] = 0;
  }
  PrintTime("Total Error", error);
  return error < target_error;
}


int main(int argc, char* argv[]) {
  // Initialize magic markers for iteration tracking (page-aligned allocations)
  iter_start_marker = (int*)malloc(PAGE_SIZE);
  iter_end_marker = (int*)malloc(PAGE_SIZE);
  algo_end_marker = (int*)malloc(PAGE_SIZE);
  
  *iter_start_marker = 0;
  *iter_end_marker = 0;
  *algo_end_marker = 0;
  
  // Print marker addresses for memory trace analysis
  printf("Magic Marker Addresses for Memory Trace:\n");
  printf("  iter_start_marker:  0x%lx\n", (unsigned long)iter_start_marker);
  printf("  iter_end_marker:    0x%lx\n", (unsigned long)iter_end_marker);
  printf("  algo_end_marker:    0x%lx\n", (unsigned long)algo_end_marker);
  printf("\n");
  
  CLPageRank cli(argc, argv, "pagerank", 1e-4, 20);
  if (!cli.ParseArgs())
    return -1;
  Builder b(cli);
  Graph g = b.MakeGraph();
  auto PRBound = [&cli] (const Graph &g) {
    return PageRankPullGS(g, cli.max_iters(), cli.tolerance(), cli.logging_en());
  };
  auto VerifierBound = [&cli] (const Graph &g, const pvector<ScoreT> &scores) {
    return PRVerifier(g, scores, cli.tolerance());
  };
  BenchmarkKernel(cli, g, PRBound, PrintTopScores, VerifierBound);
  
  // Cleanup markers
  free((void*)iter_start_marker);
  free((void*)iter_end_marker);
  free((void*)algo_end_marker);
  
  return 0;
}
