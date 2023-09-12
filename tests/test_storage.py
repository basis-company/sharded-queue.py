from pytest import mark

from sharded_queue import RuntimeStorage


@mark.asyncio
async def test_storage() -> None:
    storage = RuntimeStorage()
    await storage.append('tester', 'q')
    await storage.append('tester', 'w')
    await storage.append('tester', 'e')
    await storage.append('tester', 'r', 't', 'y')
    assert await storage.length('tester') == 6
    assert await storage.length('tester2') == 0
    assert await storage.range('tester', 1) == ['q']
    assert await storage.range('tester', 2) == ['q', 'w']
    assert await storage.range('tester', 3) == ['q', 'w', 'e']
    assert await storage.pop('tester', 1) == ['q']
    assert await storage.range('tester', 1) == ['w']
    assert await storage.range('tester', 2) == ['w', 'e']
    assert await storage.pop('tester', 2) == ['w', 'e']
    assert await storage.pop('tester', 10) == ['r', 't', 'y']
    assert await storage.pop('tester', 1) == []
