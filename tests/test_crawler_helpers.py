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

    def test_fetch_revalidates_redirect_target_and_does_not_follow_automatically(self):
        redirect = mock.Mock()
        redirect.is_redirect = True
        redirect.headers = {"Location": "http://127.0.0.1/private"}
        redirect.close = mock.Mock()
        with mock.patch.object(research, "is_public_host", side_effect=lambda host: host != "127.0.0.1"), \
             mock.patch.object(research.requests, "get", return_value=redirect) as get:
            data, content_type = research.fetch("https://example.com/start")
        self.assertIsNone(data)
        self.assertIsNone(content_type)
        get.assert_called_once()
        self.assertFalse(get.call_args.kwargs["allow_redirects"])
        redirect.close.assert_called_once()


    def test_fetch_rejects_declared_oversize_and_closes_response(self):
        response = mock.Mock()
        response.is_redirect = False
        response.headers = {"Content-Length": str(research.MAX_RESPONSE_BYTES + 1)}
        response.raise_for_status = mock.Mock()
        response.close = mock.Mock()
        with mock.patch.object(research, "is_public_host", return_value=True), \
             mock.patch.object(research.requests, "get", return_value=response):
            data, content_type = research.fetch("https://example.com/large")
        self.assertIsNone(data)
        self.assertIsNone(content_type)
        response.raise_for_status.assert_called_once()
        response.close.assert_called_once()


    def test_normalize_brackets_ipv6_literal(self):
        self.assertEqual(
            research.normalize_url("https://[2001:4860:4860::8888]/path#fragment"),
            "https://[2001:4860:4860::8888]/path",
        )

    def test_normalize_strips_matching_default_port(self):
        self.assertEqual(research.normalize_url("http://Example.com:80/a"), "http://example.com/a")
        self.assertEqual(research.normalize_url("https://Example.com:443/a"), "https://example.com/a")

    def test_normalize_rejects_wrong_scheme_port_pair(self):
        self.assertIsNone(research.normalize_url("http://example.com:443/a"))
        self.assertIsNone(research.normalize_url("https://example.com:80/a"))


if __name__ == "__main__":
    unittest.main()
