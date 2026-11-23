# Security Policy

mqBackRest handles database credentials, encryption keys and full copies of production data.
We take vulnerabilities seriously.

## Reporting

Please report vulnerabilities privately through GitHub's **"Report a vulnerability"**
button (Security → Advisories) on this repository. Include steps to reproduce and the affected version.
You will get an acknowledgement within 72 hours.

## Scope

Of particular interest: weaknesses in the encryption format (`internal/encrypt`), key handling,
credential exposure (process arguments, logs, temp files), and anything that could make a corrupt
or incomplete backup look valid.
