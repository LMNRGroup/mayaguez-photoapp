# Gallery Regression Testing

Use this when you need to find the first commit that made the Raspberry Pi / Yodeck gallery unstable.

Do not run `git bisect` automatically from the app. This is a manual testing workflow.

## Goal

Find the first bad commit between:

- a known broken commit such as `eb4071a`
- or current `HEAD`
- and an older known-good commit from before the template / overlay runtime work

## Start By Inspecting Relevant History

```bash
git log --oneline --decorate --all -- gallery.html admin.html server/index.js
```

Look for:

- the last commit you believe was stable on Raspberry Pi
- commits where template runtime, overlay polling, slideshow recovery, or telemetry changed

## Start Bisect

Mark the current state as bad:

```bash
git bisect start
git bisect bad HEAD
```

Then mark an older known-good commit:

```bash
git bisect good <OLD_GOOD_COMMIT>
```

Replace `<OLD_GOOD_COMMIT>` with the commit hash you believe was still stable on the Pi.

## Test Each Bisect Step

For each commit that `git bisect` checks out:

1. Deploy or run the gallery build for that commit.
2. Test the gallery on the Raspberry Pi / Yodeck player if possible.
3. If Pi testing is not immediately available, do a quick local sanity test first, but treat Pi behavior as the real result.

Suggested checks:

- does the gallery start rotating photos?
- does it freeze after 3–5 minutes?
- does QR/waiting appear incorrectly?
- do overlays or template runtime make it worse?
- does the page keep visually advancing on the Pi?

Then mark the result:

If the tested commit works:

```bash
git bisect good
```

If the tested commit is broken:

```bash
git bisect bad
```

Repeat until Git identifies the first bad commit.

## When Bisect Finishes

Git will print the first bad commit.

Record:

- commit hash
- commit message
- what exact Pi behavior failed
- whether Safari or desktop Chrome behaved differently

## Exit Bisect

Always reset when finished:

```bash
git bisect reset
```

## Notes

- Current broken candidate: `eb4071a` or current `HEAD`
- Prefer a real Pi/Yodeck validation for each step
- Keep notes on which commits were `good` and `bad`
- If the problem is timing-sensitive, let the gallery run for at least several minutes before judging a commit
