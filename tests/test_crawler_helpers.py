import unittest
from unittest import mock

import research


class CrawlerHelperTests(unittest.TestCase):
    def setUp(self):
        with research.visited_lock:
            research.visited_urls.clear()

    def test_normalize_accepts_http_and_removes_fragment(self):
        self.assertEqual(research.normalize_url("HTTPS://Example.COM/path#section"), "https://example.com/path")


    def test_normalize_rejects_non_http(self):
        self.assertIsNone(research.normalize_url("file:///etc/passwd"))
        self.assertIsNone(research.normalize_url("javascript:alert(1)"))

    def test_normalize_rejects_embedded_credentials(self):
        self.assertIsNone(research.normalize_url("https://user:pass@example.com"))

    def test_public_host_rejects_private_ip(self):
        with mock.patch.object(research.socket, "getaddrinfo", return_value=[(None, None, None, None, ("127.0.0.1", 0))]):
            self.assertFalse(research.is_public_host("localhost"))

    def test_public_host_accepts_public_ip(self):
        with unittest.mock.patch.object(research.socket, "getaddrinfo", return_value=[(None, None, None, None, ("8.8.8.8", 0))]):
            self.assertTrue(research.is_public_host("dns.google"))

    def test_queue_deduplicates_and_respects_cap(self):
        self.assertTrue(research.enqueue_url("https://example.com/a", 0, 2))
        self.assertFalse(research.enqueue_url("https://example.com/a#fragment", 1, 2))
        self.assertTrue(research.enqueue_url("https://example.com/b", 0, 2))
        self.assertFalse(research.enqueue_url("https://example.com/c", 0, 2))
        self.assertEqual(research.visited_urls, {"https://example.com/a", "https://example.com/b"})
        while not research.task_queue.empty():
            research.task_queue.get_nowait()
            research.task_queue.task_done()


if __name__ == "__main__":
    unittest.main()
