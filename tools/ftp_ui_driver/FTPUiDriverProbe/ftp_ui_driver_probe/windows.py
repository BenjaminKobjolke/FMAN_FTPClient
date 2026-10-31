"""Qt-independent descriptions and lookup for fman top-level windows."""

from typing import Any, Iterable


def describe_window(
    window: object,
    label_class: type,
    button_class: type,
    progress_class: type,
) -> dict[str, Any]:
    """Return visible labels, progress and button state for one window."""
    texts = [
        text for child in window.findChildren(label_class)
        if child.isVisible() and (text := child.text().strip())
    ]
    buttons = [
        {'text': text, 'enabled': bool(child.isEnabled())}
        for child in window.findChildren(button_class)
        if child.isVisible() and (text := child.text().strip())
    ]
    progress = next((
        child for child in window.findChildren(progress_class)
        if child.isVisible()
    ), None)
    result: dict[str, Any] = {
        'title': window.windowTitle(),
        'texts': texts,
        'buttons': buttons,
    }
    if progress is not None:
        result['value'] = progress.value()
        result['maximum'] = progress.maximum()
    return result


def find_window(windows: Iterable[object], title: str) -> object:
    """Find one visible top-level window by case-insensitive title substring."""
    target = title.casefold()
    matches = [
        window for window in windows
        if window.isVisible() and target in window.windowTitle().casefold()
    ]
    if not matches:
        raise ValueError('No visible window matches %r' % title)
    if len(matches) > 1:
        raise ValueError('Multiple visible windows match %r' % title)
    return matches[0]
