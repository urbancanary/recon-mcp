-- ============================================================================
-- recon_uploads.coverage_note — what the parser could NOT read from the file
-- ============================================================================
-- Target: Athena Supabase project (iociqthaxysqqqamonqa)
-- PREPARED ONLY — not applied. Never apply a migration from a lane; a human
-- runs this in the Supabase SQL Editor.
--
-- WHY. nav_parser and bbg_parser both swallow unreadable sections (a missing
-- header, a sheet in an unrecognised shape, a column the export does not
-- carry) and log a warning. The valuation still stores, and nothing on the
-- page distinguishes it from a complete one. Both parsers now return
-- `parse_coverage` (a list of {section, detail, count}); the upload handlers
-- pass it to store_raw_upload, which writes the rendered one-line note here.
--
-- This is also what makes the parsers quotable: the redacted GCRIF (InvestOne
-- NAV) and BBG PORT samples are wanted by the rebuild audit precisely because
-- coverage cannot be assessed without them. Until they arrive, this column
-- records, per ingested file, which sections came back empty — so a coverage
-- gap is visible on the upload rather than inferred from a log nobody reads.
--
-- Additive and nullable: existing rows and existing writers are unaffected,
-- and the code passes NULL when the parse reported full coverage.

ALTER TABLE recon_uploads
    ADD COLUMN IF NOT EXISTS coverage_note TEXT;

COMMENT ON COLUMN recon_uploads.coverage_note IS
    'Sections the parser could not read from this file (nav_parser/bbg_parser '
    'parse_coverage). NULL = full structural coverage. Never a financial value.';
