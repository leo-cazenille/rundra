# SoftwareX manuscript draft

## Objective

Register `publication/rundra-overleaf` as a Git submodule and assemble an
Original Software Publication draft for SoftwareX. Keep the manuscript and its
assets in the existing Overleaf-linked repository.

## Milestones

1. Preserve the existing child repository and register its GitHub origin as a
   submodule. Leave unrelated repository changes untouched.
2. Replace the starter with an Elsevier LaTeX manuscript, bibliography, vector
   architecture diagram, scientific illustration, and compact evidence ledger.
3. Document the manuscript build, evidence limitations, and author/submission
   information that remains to be supplied.
4. Compile the manuscript and commit the child repository, then commit the
   parent submodule pointer and this plan. Do not push without authorization.

## Evidence policy

- Describe the implementation at development commit
  `710396582fbc44252f4f8972a7cf8ddabc68d4f2`, not all features as part of the
  published 0.1.7 distribution.
- Use documented architecture and scheduler capabilities for implementation
  descriptions. Distinguish Docker lifecycle tests from physical-cluster runs.
- Use retained Pogosim summaries and campaign records as observations, not as
  independently repeated benchmarks or evidence of comparative performance.
- State that the campaign's recorded task outcomes and retrieval states do not
  independently establish scientific validity or hardware-independent
  reproducibility.
- Do not invent authors, affiliations, funding, competing-interest statements,
  archival identifiers, measurements, or independent adoption.
- Cite primary sources for related software. Mark missing submission metadata
  explicitly and keep an actionable checklist beside the manuscript.

## Acceptance criteria

- LaTeX and bibliography compile into a local PDF without fatal errors.
- All referenced diagrams, data, and bibliography files live in the submodule.
- Generated compilation files are ignored.
- The parent records the manuscript commit through a proper Git submodule.
- The draft is clearly identified as a draft requiring author review and a
  final check against SoftwareX's current author instructions.

## Validation scope

This task changes publication material and Git metadata only. Compile the
manuscript and run whitespace checks on the affected repositories; Python
execution tests and cluster submissions are outside this task's scope.
