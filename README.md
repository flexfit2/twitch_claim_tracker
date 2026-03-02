Twitch Chat Narrative Analysis Framework

A modular pipeline for ingesting, structuring, and analyzing Twitch VOD chat logs.

This project provides tools for:

Chat ingestion from Twitch VOD archives

Structured storage in SQLite

Conversation graph extraction (reply-based clustering)

Self-referential statement detection

Temporal inference and timeline reconstruction

Narrative clustering

The system is configuration-driven and can be applied to any Twitch channel.

Features
1. Chat Ingestion

Uses TwitchDownloaderCLI

Stores raw and cleaned messages

Tracks processed VODs

Supports incremental updates for ongoing streams

2. Conversation Detection

Two conversation types:

User Conversations – Direct interaction with a target user

Topic Conversations – Clusters of messages about a target

Conversations are stored relationally:

conversations

conversation_messages

3. Claim Extraction

Extracts self-referential statements using:

First-person pronoun detection

Sentence segmentation

Epistemic filtering (e.g., excluding "I think", "I guess")

Stored in claims table.

4. Temporal Modeling

Identifies:

Absolute years

Year ranges

Age statements

Relative time references ("X years ago")

Produces:

Estimated demographic baseline

Timeline reconstruction

Range conflict detection

Configuration

Environment variables:

TWITCH_USERNAME=
TWITCH_CLIENT_ID=
TWITCH_ACCESS_TOKEN=
TARGET_USER=
TARGET_ALIASES=
TOPIC_DIRECT_SIGNALS=
TOPIC_SOFT_SIGNALS=

No hardcoded identities are present in the codebase.

Use Cases

Computational discourse analysis

Narrative identity modeling

Digital ethnography

Longitudinal chat behavior studies

Ethical Considerations

This tool is intended for research and archival purposes.

Users are responsible for complying with:

Twitch Terms of Service

Privacy regulations

Ethical data usage standards

Architecture Overview
Twitch API → Chat JSON → SQLite → Conversation Graph →
Claim Extraction → Temporal Analysis → Reports