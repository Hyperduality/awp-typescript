# AGENTS.md

awp-typescript is an agent SDK for the Agent World Protocol on Node 20+, published on npm as `@hyperduality/awp`. It was written from the specification and the canonical schemas alone. Keep it that way: don't port code from awp-python or awp-sim. It targets one specification revision. `schemas/` vendors that revision's schemas, frame vectors, and lifecycle table from the git ref in `schemas/source.json`, and `SPEC_REVISION` in `src/version.ts` names it.

## Checks

```bash
npm ci
npm run typecheck
npm test
npm run check-schemas -- --from ../agent-world-protocol
```

CI runs these on Node 20, 22, and 24. It also runs awp-conformance against `awp-demo` in both time models, and fails unless the claim is AWP-conformant.

## Code

- Cite a requirement ID (`AWP-XXX-NNN`) where the code implements it, and in the name of the test that shows it. Otherwise, comment only what the code can't say.
- Never edit `schemas/` by hand; re-vendor with `npm run sync-schemas`.

## Conformance

`conformance/` holds the reports behind the README's claim, plus the evidence for their `manual` rows. The reports must come from the suite version CI runs. After bumping that pin in `.github/workflows/ci.yml`, regenerate them with the commands in `conformance/README.md`. Then update the versions named there and in the README.

## Commits and pull requests

- Branch from `main` and open a pull request. Merge once CI passes.
- Write the title as one plain sentence in sentence case, with no trailing period, saying what changed: `Publish to npm as @hyperduality/awp`. When the change is part of a release, end it with the version: `(0.1.0-alpha.2)`.
- Add a body only when the title can't carry the reason: one or two short sentences.
- Write commits the way a person on the project would. No `Co-Authored-By` trailers, no "Generated with" lines, and no other mention of AI tools, in commits or in PRs.
- The PR title matches the commit title, and the description is a few lines at most.

## Moving to a new draft revision

1. Run `npm run sync-schemas -- --ref spec-v0.1-draft.N`.
2. Update `SPEC_REVISION` in `src/version.ts`.
3. Fix whatever the tests report.

## Releasing

1. In a pull request:
   - Set the version with `npm version --no-git-tag-version <version>`, for example `0.1.0-alpha.2`.
   - Add a `CHANGELOG.md` entry naming the targeted revision and the user-visible changes.
2. Once it is merged, tag the merge commit and push the tag:

   ```bash
   git tag -a v0.1.0-alpha.2 -m "awp-typescript 0.1.0-alpha.2 (AWP 0.1-draft.N)"
   git push origin v0.1.0-alpha.2
   ```

3. `release.yml` checks that the tag matches the version, then tests and packs the package.
   - It publishes through npm trusted publishing once someone approves the `npm` environment. A maintainer gives that approval. Agents never approve deployments, and never publish with a token.
   - A prerelease is published under its prerelease id as the dist-tag (`alpha`), and a release under `latest`. A version that is already on npm is skipped.
