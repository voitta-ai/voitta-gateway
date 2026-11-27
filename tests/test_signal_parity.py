"""Signal features manhattan-logic has, now in the gateway: everything a Signal message can carry,
inbound and outbound, plus stickers, contacts and group invitations. Fake signal-cli, real gateway,
every response and event checked against the specs."""
import pytest

from .test_signal import ACCOUNT, PERSON, config, envelope, fake, up  # noqa: F401  (fake is a fixture)

GROUP = "GROUP1"
OTHER_GROUP = "R1JPVVAtT1RIRVI="


def dm(ts, **data):
    return envelope(PERSON, ts, dataMessage={"timestamp": ts, **data})


async def received(gw, fake, ts, **data):
    await fake.receive(dm(ts, **data))
    return (await gw.wait_event("message.received"))["data"]


async def test_inbound_everything(gateway, fake):
    gw = await gateway(config(fake))
    await up(gw, fake)

    # formatting, voice note and view-once on files, link preview, sticker
    d = await received(gw, fake, 1001, message="Bold and link https://example.com",
                       textStyles=[{"start": 0, "length": 4, "style": "BOLD"}],
                       previews=[{"url": "https://example.com", "title": "Example", "description": "An example"}])
    assert d["parts"][0]["styles"] == [{"start": 0, "length": 4, "style": "BOLD"}]
    assert d["parts"][1] == {"type": "link_preview", "url": "https://example.com", "title": "Example",
                             "description": "An example"}
    d = await received(gw, fake, 1002, viewOnce=True, attachments=[
        {"id": "missing-1", "contentType": "audio/aac", "filename": None, "size": 900, "isVoiceNote": True}])
    f = d["parts"][0]
    assert f["voice_note"] is True and f["view_once"] is True and f["missing"] is True
    d = await received(gw, fake, 1003, sticker={"packId": "a" * 32, "stickerId": 4})
    assert d["parts"] == [{"type": "sticker", "pack_id": "a" * 32, "sticker_id": 4}]

    # polls: created, voted on (authorUuid is the POLL's author), ended
    d = await received(gw, fake, 1004, pollCreate={"question": "Lunch?", "allowMultiple": False,
                                                   "options": ["pizza", "sushi"]})
    assert d["parts"] == [{"type": "poll", "question": "Lunch?", "options": ["pizza", "sushi"], "multiple": False}]
    d = await received(gw, fake, 1005, pollVote={"authorUuid": PERSON, "targetSentTimestamp": 1004,
                                                 "optionIndexes": [1], "voteCount": 2})
    assert d["parts"] == [{"type": "poll_vote", "poll": f"{PERSON}:1004", "options": [1], "vote_count": 2}]
    d = await received(gw, fake, 1006, pollTerminate={"targetSentTimestamp": 1004})
    assert d["parts"] == [{"type": "poll_end", "poll": f"{PERSON}:1004"}]

    # pins, a shared contact, the timer, a payment, something not modelled yet
    d = await received(gw, fake, 1007, pinMessage={"targetAuthorUuid": PERSON, "targetSentTimestamp": 1001,
                                                   "pinDurationSeconds": 3600})
    assert d["parts"] == [{"type": "pin", "target": f"{PERSON}:1001", "duration_s": 3600}]
    d = await received(gw, fake, 1008, unpinMessage={"targetAuthorUuid": PERSON, "targetSentTimestamp": 1001})
    assert d["parts"] == [{"type": "unpin", "target": f"{PERSON}:1001"}]
    d = await received(gw, fake, 1009, contacts=[{"name": {"given": "Ada", "family": "Lovelace"},
                                                  "phone": [{"value": "+15550009999", "type": "MOBILE"}],
                                                  "email": [], "organization": "Analytical"}])
    assert d["parts"] == [{"type": "contact", "name": "Ada Lovelace", "phones": ["+15550009999"],
                           "emails": [], "organization": "Analytical"}]
    d = await received(gw, fake, 1010, isExpirationUpdate=True, expiresInSeconds=604800)
    assert d["parts"] == [{"type": "disappearing", "seconds": 604800}]
    d = await received(gw, fake, 1011, payment={"note": "for lunch"})
    assert d["parts"] == [{"type": "payment", "note": "for lunch"}]
    d = await received(gw, fake, 1012, groupCallUpdate={"eraId": "x"})
    assert d["parts"] == [{"type": "other", "kind": "group_call", "data": {}}]


async def test_contact_names_on_senders(gateway, fake):
    fake.contacts = [{"uuid": PERSON, "number": "+15550002222", "name": "Pat (work)", "isBlocked": False}]
    gw = await gateway(config(fake))
    await up(gw, fake)
    d = await received(gw, fake, 2001, message="hi")
    assert d["sender"]["contact_name"] == "Pat (work)"
    r = await gw.client.get("/v1/signal/contacts")
    assert r.json()["contacts"] == [{"id": PERSON, "number": "+15550002222", "name": "Pat (work)", "blocked": False}]


async def test_outbound_parts(gateway, fake):
    gw = await gateway(config(fake))
    await up(gw, fake)
    cid = (await received(gw, fake, 3001, message="hello"))["conversation_id"]
    att = (await gw.client.post("/v1/attachments", content=b"\x89PNGdata", headers={"Content-Type": "image/png"})).json()

    async def send(parts):
        r = await gw.client.post("/v1/messages", json={"to": {"conversation_id": cid}, "parts": parts, "wait_s": 5})
        assert r.status_code == 200, r.text
        return fake.methods("send")[-1]

    p = await send([{"type": "text", "text": "a secret"}, {**att, "view_once": True}])
    assert p["viewOnce"] is True and len(p["attachments"]) == 1
    p = await send([{"type": "sticker", "pack_id": "b" * 32, "sticker_id": 7}])
    assert p["sticker"] == f"{'b' * 32}:7"
    p = await send([{"type": "text", "text": "see https://example.com/x"},
                    {"type": "link_preview", "url": "https://example.com/x", "title": "X"}])
    assert p["previewUrl"] == "https://example.com/x" and p["previewTitle"] == "X"

    # refused up front, nothing queued
    for parts, code in (([{"type": "text", "text": "no link"}, {"type": "link_preview", "url": "https://a.b"}], "bad_preview"),
                        ([{"type": "sticker", "pack_id": "b" * 32, "sticker_id": "x"}], "bad_sticker"),
                        ([{"type": "poll", "question": "Q?", "options": ["only one"]}], "bad_poll"),
                        ([{"type": "poll", "question": "Q?", "options": ["a", "b"]}, {"type": "text", "text": "x"}],
                         "bad_parts")):
        r = await gw.client.post("/v1/messages", json={"to": {"conversation_id": cid}, "parts": parts})
        assert r.status_code == 422 and r.json()["error"]["code"] == code, (parts, r.text)


async def test_polls_pins_and_timer(gateway, fake):
    gw = await gateway(config(fake))
    await up(gw, fake)
    cid = (await received(gw, fake, 4001, message="hello"))["conversation_id"]

    r = await gw.client.post("/v1/messages", json={"to": {"conversation_id": cid}, "wait_s": 5, "parts": [
        {"type": "poll", "question": "Lunch?", "options": ["pizza", "sushi", "salad"], "multiple": False}]})
    assert r.status_code == 200, r.text
    poll = r.json()["message_id"]
    created = fake.methods("sendPollCreate")[0]
    assert created == {"recipient": [PERSON], "question": "Lunch?", "option": ["pizza", "sushi", "salad"],
                       "noMulti": True}
    poll_ts = fake.ts

    # two votes from this account: Signal keeps the higher count, so it must grow
    for opts in ([0], [2]):
        r = await gw.client.post(f"/v1/messages/{poll}/vote", json={"options": opts})
        assert r.status_code == 202, r.text
        await gw.wait_event("operation.done", pred=lambda e: e["data"]["op"] == "poll_vote")
    votes = fake.methods("sendPollVote")
    assert [v["voteCount"] for v in votes] == [1, 2] and votes[1]["option"] == [2]
    assert votes[0]["pollAuthor"] == ACCOUNT and votes[0]["pollTimestamp"] == poll_ts
    r = await gw.client.post(f"/v1/messages/{poll}/vote", json={"options": [3]})
    assert r.status_code == 422 and "3 options" in r.json()["error"]["message"]

    r = await gw.client.post(f"/v1/messages/{poll}/end_poll", json={})
    assert r.status_code == 202
    await gw.wait_event("operation.done", pred=lambda e: e["data"]["op"] == "poll_end")
    assert fake.methods("sendPollTerminate") == [{"recipient": [PERSON], "pollTimestamp": poll_ts}]

    # someone else's message: pin and unpin work, ending it as a poll does not
    theirs = (await received(gw, fake, 4002, message="pin me"))["message_id"]
    r = await gw.client.post(f"/v1/messages/{theirs}/pin", json={"duration_s": 86400})
    assert r.status_code == 202
    await gw.wait_event("operation.done", pred=lambda e: e["data"]["op"] == "pin")
    assert fake.methods("sendPinMessage")[0] == {"recipient": [PERSON], "targetAuthor": PERSON,
                                                 "targetTimestamp": 4002, "pinDuration": 86400}
    await gw.client.post(f"/v1/messages/{theirs}/unpin", json={})
    await gw.wait_event("operation.done", pred=lambda e: e["data"]["op"] == "unpin")
    r = await gw.client.post(f"/v1/messages/{theirs}/end_poll", json={})
    assert r.status_code == 422 and r.json()["error"]["code"] == "not_a_poll"
    r = await gw.client.post(f"/v1/messages/{theirs}/pin", json={"duration_s": 0})
    assert r.status_code == 422 and r.json()["error"]["code"] == "bad_duration"

    # disappearing timer: a DM goes through updateContact, a group through updateGroup
    r = await gw.client.post(f"/v1/conversations/{cid}/disappearing", json={"seconds": 3600})
    assert r.status_code == 202, r.text
    await gw.wait_event("operation.done", pred=lambda e: e["data"]["op"] == "disappearing")
    assert fake.methods("updateContact") == [{"recipient": PERSON, "expiration": 3600}]
    await fake.receive(envelope(PERSON, 4003, dataMessage={"timestamp": 4003, "message": "@bot hi",
                                                           "groupInfo": {"groupId": GROUP},
                                                           "mentions": [{"uuid": fake.aci, "start": 0, "length": 1}]}))
    gcid = (await gw.wait_event("message.received"))["data"]["conversation_id"]
    await gw.client.post(f"/v1/conversations/{gcid}/disappearing", json={"seconds": 0})
    await gw.wait_event("operation.done", pred=lambda e: e["data"]["op"] == "disappearing")
    assert fake.methods("updateGroup")[-1] == {"groupId": GROUP, "expiration": 0}
    r = await gw.client.post(f"/v1/conversations/{cid}/disappearing", json={"seconds": -1})
    assert r.status_code == 422


async def test_stickers(gateway, fake):
    fake.sticker_packs = [{"packId": "c" * 32, "title": "Cats", "author": "A", "installed": True,
                           "url": "https://signal.art/addstickers/#pack_id=c&pack_key=k",
                           "stickers": [{"id": 0, "emoji": "😺", "contentType": "image/webp"}]}]
    gw = await gateway(config(fake))
    await up(gw, fake)
    packs = (await gw.client.get("/v1/signal/stickers")).json()["packs"]
    assert packs[0]["pack_id"] == "c" * 32 and packs[0]["stickers"][0]["emoji"] == "😺"
    r = await gw.client.get(f"/v1/signal/stickers/{'c' * 32}/0")
    assert r.status_code == 200 and r.headers["content-type"] == "image/webp" and r.content.startswith(b"RIFF")
    assert (await gw.client.get("/v1/signal/stickers/notapack/0")).status_code == 422
    r = await gw.client.post("/v1/signal/stickers", json={"url": "https://signal.art/addstickers/#pack_id=d&pack_key=k"})
    assert r.status_code == 200 and fake.methods("addStickerPack")[0]["uri"].startswith("https://signal.art/")
    assert (await gw.client.post("/v1/signal/stickers", json={"url": "https://evil.example/x"})).status_code == 422


async def test_group_invites(gateway, fake):
    """An invitation to an unlisted group: one admin notice, nothing stored, nothing joined by itself."""
    fake.groups = [{"id": OTHER_GROUP, "name": "Book club", "isMember": False,
                    "pendingMembers": [{"uuid": fake.aci, "number": ACCOUNT}], "members": []}]
    gw = await gateway(config(fake))
    await up(gw, fake)
    for ts in (5001, 5002):                                  # two messages, one notice
        await fake.receive(envelope(PERSON, ts, dataMessage={"timestamp": ts, "message": "join us",
                                                             "groupInfo": {"groupId": OTHER_GROUP}}))
    inv = await gw.wait_event("group.invited")
    assert inv["data"] == {"channel": "signal", "group": OTHER_GROUP, "name": "Book club", "listed": False,
                           "addressed_to": ""}
    assert len(await gw.events("group.invited")) == 1
    assert not await gw.events("message.received")           # the group stays closed
    assert gw.gw.store.one("SELECT COUNT(*) AS n FROM raw_inbound")["n"] == 0

    r = await gw.client.post(f"/v1/signal/groups/{OTHER_GROUP}/accept")
    assert r.status_code == 200 and fake.methods("updateGroup") == [{"groupId": OTHER_GROUP}]
    r = await gw.client.post("/v1/signal/groups/join", json={"uri": "https://signal.group/#CjQKIA"})
    assert r.status_code == 200 and r.json() == {"group": "Z3JvdXAtam9pbmVk"}
    assert (await gw.client.post("/v1/signal/groups/join", json={"uri": "https://evil.example"})).status_code == 422


async def test_account_ops_need_admin(gateway, fake):
    import os
    os.environ["AGENT_TOKEN"] = "agent-token"
    gw = await gateway(config(fake, consumers={
        "agent": {"token_secret": "TEST_TOKEN", "scopes": ["admin"], "events": ["*"]},
        "bot": {"token_secret": "AGENT_TOKEN", "scopes": ["events:read", "messages:send"], "events": ["*"]}}))
    await up(gw, fake)
    bot = {"Authorization": "Bearer agent-token"}
    assert (await gw.client.get("/v1/signal/contacts", headers=bot)).status_code == 403
    assert (await gw.client.post("/v1/signal/groups/join", headers=bot, json={"uri": "https://signal.group/#x"})).status_code == 403
    assert (await gw.client.get("/v1/signal/stickers", headers=bot)).status_code == 200
