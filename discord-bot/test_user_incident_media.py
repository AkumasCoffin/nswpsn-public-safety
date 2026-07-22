"""
Stub-based test for user-incident units + photo rendering.

discord.py isn't installed in every dev/CI environment, so this injects a
minimal recording stub for the `discord` module before importing embeds,
then walks the built container to assert the units line and the photo
media gallery are present with correctly-formed Cloudflare URLs.

Run: python test_user_incident_media.py
"""
import sys
import types
import re


# --- Minimal recording stub for the bits of discord.py embeds.py uses ---
def _make_discord_stub():
    discord = types.ModuleType('discord')

    class _Rec:
        def __init__(self, **kw):
            self.__dict__.update(kw)
            self.items = []
            self.children = []

        def add_item(self, item=None, **kw):
            self.items.append(item if item is not None else kw)
            self.children.append(item if item is not None else kw)
            return self

    class Container(_Rec):
        pass

    class TextDisplay(_Rec):
        pass

    class ActionRow(_Rec):
        pass

    class Button(_Rec):
        pass

    class MediaGallery:
        def __init__(self, *items):
            self.gallery_items = list(items)

    class MediaGalleryItem:
        def __init__(self, media, description=None, spoiler=False):
            self.media = media
            self.description = description

    ui = types.ModuleType('discord.ui')
    ui.Container = Container
    ui.TextDisplay = TextDisplay
    ui.ActionRow = ActionRow
    ui.Button = Button
    ui.MediaGallery = MediaGallery

    class _ButtonStyle:
        link = 'link'

    discord.ui = ui
    discord.MediaGalleryItem = MediaGalleryItem
    discord.ButtonStyle = _ButtonStyle
    sys.modules['discord'] = discord
    sys.modules['discord.ui'] = ui
    return discord


_make_discord_stub()
import embeds  # noqa: E402


def _walk_text(container):
    """All TextDisplay contents anywhere in the container tree."""
    out = []
    for item in getattr(container, 'items', []):
        if item.__class__.__name__ == 'TextDisplay':
            out.append(item.content)
        elif hasattr(item, 'items'):
            out.extend(_walk_text(item))
    return out


def _find_galleries(container):
    return [i for i in getattr(container, 'items', [])
            if i.__class__.__name__ == 'MediaGallery']


def run():
    b = embeds.EmbedBuilder()
    data = {
        'title': 'Grass fire near oval',
        'location': 'Coraki',
        'description': 'Spreading east',
        'status': 'Going',
        'type': ['Grass Fire'],
        'responding_agencies': ['RFS'],
        'units': ['P1', 'CAT7', '  ', 'CAT7'],  # blanks/dupes tolerated
        'images': [
            {'file': '/uploads/incident-images/inc-1/aaaaaaaa-1111-2222-3333-444444444444.jpg'},
            {'file': '/uploads/incident-images/inc-1/bbbbbbbb-1111-2222-3333-444444444444.png'},
            {'file': '/etc/passwd'},                       # rejected — bad shape
            {'file': '/uploads/incident-images/inc-1/c.jpg'},
            {'file': '/uploads/incident-images/inc-1/d.jpg'},
            {'file': '/uploads/incident-images/inc-1/e.jpg'},  # beyond the 4 cap
        ],
        'lat': -33.9, 'lng': 151.1,
        'created_at': '2026-07-22T00:00:00Z',
        'logs': [],
    }
    container = b.build_user_incident_container(data)
    texts = _walk_text(container)
    joined = '\n'.join(texts)

    # 1. Units line present, de-duped, blanks dropped.
    units_line = next((t for t in texts if 'Units:' in t), None)
    assert units_line, f'no units line in: {texts}'
    assert 'P1' in units_line and 'CAT7' in units_line, units_line

    # 2. A media gallery with the valid photos (capped at 4, bad path dropped).
    galleries = _find_galleries(container)
    assert len(galleries) == 1, f'expected 1 gallery, got {len(galleries)}'
    urls = [gi.media for gi in galleries[0].gallery_items]
    assert len(urls) == 4, f'expected 4 photos (cap + drop bad), got {len(urls)}: {urls}'
    for u in urls:
        assert u.startswith('https://nswpsn.forcequit.xyz/cdn-cgi/image/width=1024,'), u
        assert '/uploads/incident-images/' in u
    assert not any('/etc/passwd' in u for u in urls), 'traversal path leaked into a URL'

    # 3. The helper rejects malformed paths outright.
    assert embeds.build_incident_image_url('/etc/passwd') is None
    assert embeds.build_incident_image_url('../../x.jpg') is None
    assert embeds.build_incident_image_url(
        '/uploads/incident-images/inc-1/a.jpg'
    ) == 'https://nswpsn.forcequit.xyz/cdn-cgi/image/width=1024,quality=80,format=auto/uploads/incident-images/inc-1/a.jpg'

    # 4. No units / no images → neither section rendered, no crash.
    bare = b.build_user_incident_container({'title': 'x', 'created_at': '2026-07-22T00:00:00Z'})
    assert not _find_galleries(bare)
    assert not any('Units:' in t for t in _walk_text(bare))

    print('PASS: units + photo gallery render, bad paths rejected')


if __name__ == '__main__':
    run()
