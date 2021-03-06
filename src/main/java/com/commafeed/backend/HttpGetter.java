package com.commafeed.backend;

import java.io.IOException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.client.params.HttpClientParams;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.impl.client.DecompressingHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.SystemDefaultHttpClient;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpGetter {

	private static Logger log = LoggerFactory.getLogger(HttpGetter.class);

	private static SSLContext SSL_CONTEXT = null;
	static {
		try {
			SSL_CONTEXT = SSLContext.getInstance("TLS");
			SSL_CONTEXT.init(new KeyManager[0],
					new TrustManager[] { new DefaultTrustManager() },
					new SecureRandom());
		} catch (Exception e) {
			log.error("Could not configure ssl context");
		}
	}

	private static final X509HostnameVerifier VERIFIER = new DefaultHostnameVerifier();

	public HttpResult getBinary(String url) throws ClientProtocolException,
			IOException, NotModifiedException {
		return getBinary(url, null, null);
	}

	/**
	 * 
	 * @param url
	 *            the url to retrive
	 * @param lastModified
	 *            header we got last time we queried that url, or null
	 * @param eTag
	 *            header we got last time we queried that url, or null
	 * @return
	 * @throws ClientProtocolException
	 * @throws IOException
	 * @throws NotModifiedException
	 *             if the url hasn't changed since we asked for it last time
	 */
	public HttpResult getBinary(String url, String lastModified, String eTag)
			throws ClientProtocolException, IOException, NotModifiedException {
		HttpResult result = null;
		long start = System.currentTimeMillis();

		HttpClient client = newClient();
		try {
			HttpGet httpget = new HttpGet(url);
			httpget.addHeader(HttpHeaders.ACCEPT_LANGUAGE, "en");
			httpget.addHeader(HttpHeaders.PRAGMA, "No-cache");
			httpget.addHeader(HttpHeaders.CACHE_CONTROL, "no-cache");
			httpget.addHeader(HttpHeaders.USER_AGENT,
					"CommaFeed/1.0 (http://www.commafeed.com)");

			if (lastModified != null) {
				httpget.addHeader(HttpHeaders.IF_MODIFIED_SINCE, lastModified);
			}
			if (eTag != null) {
				httpget.addHeader(HttpHeaders.IF_NONE_MATCH, eTag);
			}

			HttpResponse response = null;
			try {
				response = client.execute(httpget);
				int code = response.getStatusLine().getStatusCode();
				if (code == HttpStatus.SC_NOT_MODIFIED) {
					throw new NotModifiedException();
				} else if (code >= 300) {
					throw new HttpResponseException(code,
							"Server returned HTTP error code " + code);
				}

			} catch (HttpResponseException e) {
				if (e.getStatusCode() == HttpStatus.SC_NOT_MODIFIED) {
					throw new NotModifiedException();
				} else {
					throw e;
				}
			}
			Header lastModifiedHeader = response
					.getFirstHeader(HttpHeaders.LAST_MODIFIED);
			Header eTagHeader = response.getFirstHeader(HttpHeaders.ETAG);

			String lastModifiedResponse = lastModifiedHeader == null ? null
					: lastModifiedHeader.getValue();
			if (lastModified != null
					&& StringUtils.equals(lastModified, lastModifiedResponse)) {
				throw new NotModifiedException();
			}

			String eTagResponse = eTagHeader == null ? null : eTagHeader
					.getValue();
			if (eTag != null && StringUtils.equals(eTag, eTagResponse)) {
				throw new NotModifiedException();
			}

			HttpEntity entity = response.getEntity();
			byte[] content = null;
			if (entity != null) {
				content = EntityUtils.toByteArray(entity);
			}

			long duration = System.currentTimeMillis() - start;
			result = new HttpResult(content, lastModifiedHeader == null ? null
					: lastModifiedHeader.getValue(), eTagHeader == null ? null
					: eTagHeader.getValue(), duration);
		} finally {
			client.getConnectionManager().shutdown();
		}
		return result;
	}

	public static class HttpResult {

		private byte[] content;
		private String lastModifiedSince;
		private String eTag;
		private long duration;

		public HttpResult(byte[] content, String lastModifiedSince,
				String eTag, long duration) {
			this.content = content;
			this.lastModifiedSince = lastModifiedSince;
			this.eTag = eTag;
			this.duration = duration;
		}

		public byte[] getContent() {
			return content;
		}

		public String getLastModifiedSince() {
			return lastModifiedSince;
		}

		public String geteTag() {
			return eTag;
		}

		public long getDuration() {
			return duration;
		}

	}

	public static HttpClient newClient() {
		DefaultHttpClient client = new SystemDefaultHttpClient();

		SSLSocketFactory ssf = new SSLSocketFactory(SSL_CONTEXT, VERIFIER);
		ClientConnectionManager ccm = client.getConnectionManager();
		SchemeRegistry sr = ccm.getSchemeRegistry();
		sr.register(new Scheme("https", 443, ssf));

		HttpParams params = client.getParams();
		HttpClientParams.setCookiePolicy(params, CookiePolicy.IGNORE_COOKIES);
		HttpProtocolParams.setContentCharset(params, "UTF-8");
		HttpConnectionParams.setConnectionTimeout(params, 4000);
		HttpConnectionParams.setSoTimeout(params, 4000);
		client.setHttpRequestRetryHandler(new DefaultHttpRequestRetryHandler(0,
				false));
		return new DecompressingHttpClient(client);
	}

	public static class NotModifiedException extends Exception {
		private static final long serialVersionUID = 1L;

	}

	private static class DefaultTrustManager implements X509TrustManager {
		@Override
		public void checkClientTrusted(X509Certificate[] arg0, String arg1)
				throws CertificateException {
		}

		@Override
		public void checkServerTrusted(X509Certificate[] arg0, String arg1)
				throws CertificateException {
		}

		@Override
		public X509Certificate[] getAcceptedIssuers() {
			return null;
		}
	}

	private static class DefaultHostnameVerifier implements
			X509HostnameVerifier {

		@Override
		public void verify(String string, SSLSocket ssls) throws IOException {
		}

		@Override
		public void verify(String string, X509Certificate xc)
				throws SSLException {
		}

		@Override
		public void verify(String string, String[] strings, String[] strings1)
				throws SSLException {
		}

		@Override
		public boolean verify(String string, SSLSession ssls) {
			return true;
		}
	};
}
