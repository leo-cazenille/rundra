# Compact execution checks in Docker Slurm CI

1. Share journal file selection between full and incremental readers. Preserve
   the legacy aggregate layout and prefer a published aggregate over lane
   files for its specific attempt. Continue reading other attempts.
2. Run a small compact sweep on the disposable SSH-backed Slurm target. Fetch
   and export the parent results, retry one failed coordinate, and verify the
   exact config and lineage. Reject a changed staged config before submission.
3. Hold sequential Tasks at test-controlled barriers. Poll from separate CLI
   processes and observe a nonempty delta, then an aggregate read. Compare
   final outcomes with an independent full read from the same pre-terminal
   store snapshot. Run the fixture through Apptainer in GitHub Actions.
4. Include both cases in the Docker Slurm workflow and document its green
   result as a release prerequisite. Run default validation and the disposable
   system checks this host can support.

The tests lower the compact threshold only in their own CLI subprocesses.
Production thresholds, CLI contracts, and saved RunRecord schemas stay intact.
