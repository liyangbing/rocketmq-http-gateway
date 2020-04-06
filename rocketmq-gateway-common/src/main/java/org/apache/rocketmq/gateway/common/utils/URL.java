package org.apache.rocketmq.gateway.common.utils;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class URL implements Serializable {

    public static final String FILE = "file";
    private static final long serialVersionUID = -1985165475234910535L;
    private final String protocol;
    private final String username;
    private final String password;
    private final String host;
    private final int port;
    private final String path;
    private final Map<String, String> parameters;

    protected URL() {
        this.protocol = null;
        this.username = null;
        this.password = null;
        this.host = null;
        this.port = 0;
        this.path = null;
        this.parameters = null;
    }

    public URL(String protocol, String host, int port) {
        this(protocol, null, null, host, port, null, (Map<String, String>) null);
    }

    public URL(String protocol, String host, int port, Map<String, String> parameters) {
        this(protocol, null, null, host, port, null, parameters);
    }

    public URL(String protocol, String host, int port, String path) {
        this(protocol, null, null, host, port, path, (Map<String, String>) null);
    }

    public URL(String protocol, String host, int port, String path, Map<String, String> parameters) {
        this(protocol, null, null, host, port, path, parameters);
    }

    public URL(String protocol, String username, String password, String host, int port, String path) {
        this(protocol, username, password, host, port, path, (Map<String, String>) null);
    }

    public URL(String protocol, String username, String password, String host, int port, String path,
               Map<String, String> parameters) {
        if ((username == null || username.isEmpty()) && password != null && password.length() > 0) {
            throw new IllegalArgumentException("Invalid url, password without username!");
        }
        this.protocol = protocol;
        this.username = username;
        this.password = password;
        this.host = host;
        this.port = (port < 0 ? 0 : port);
        this.path = path;
        // trim the beginning "/"
        while (path != null && path.startsWith("/")) {
            path = path.substring(1);
        }
        if (parameters == null) {
            parameters = new HashMap<String, String>();
        } else {
            parameters = new HashMap<String, String>(parameters);
        }
        this.parameters = Collections.unmodifiableMap(parameters);
    }

    /**
     * 把字符串转化成URL对象
     *
     * @param url
     * @return
     */
    public static URL valueOf(String url) {
        if (url == null) {
            return null;
        }
        url = url.trim();
        if (url.isEmpty()) {
            return null;
        }
        String protocol = null;
        String username = null;
        String password = null;
        String host = null;
        int port = 0;
        String path = null;
        Map<String, String> parameters = null;


        int j = 0;
        int i = url.indexOf(')');
        if (i >= 0) {
            i = url.indexOf('?', i);
        } else {
            i = url.indexOf("?");
        }
        if (i >= 0) {
            if (i < url.length() - 1) {
                String[] parts = url.substring(i + 1).split("\\&");
                parameters = new HashMap<String, String>();
                for (String part : parts) {
                    part = part.trim();
                    if (part.length() > 0) {
                        j = part.indexOf('=');
                        if (j > 0) {
                            if (j == part.length() - 1) {
                                parameters.put(part.substring(0, j), "");
                            } else {
                                parameters.put(part.substring(0, j), part.substring(j + 1));
                            }
                        } else if (j == -1) {
                            parameters.put(part, part);
                        }
                    }
                }
            }
            url = url.substring(0, i);
        }
        i = url.indexOf("://");
        if (i > 0) {
            protocol = url.substring(0, i);
            url = url.substring(i + 3);
        } else if (i < 0) {
            // case: file:/path/to/file.txt
            i = url.indexOf(":/");
            if (i > 0) {
                protocol = url.substring(0, i);
                // 保留路径符号“/”
                url = url.substring(i + 1);
            }
        }
        if (protocol == null || protocol.isEmpty()) {
            throw new IllegalStateException("url missing protocol: " + url);
        }
        if (protocol.equals(FILE)) {
            path = url;
            url = "";
        } else {
            i = url.lastIndexOf(')');
            if (i >= 0) {
                i = url.indexOf('/', i);
            } else {
                i = url.indexOf("/");
            }
            if (i >= 0) {
                path = url.substring(i + 1);
                url = url.substring(0, i);
            }
        }
        i = url.indexOf('(');
        if (i >= 0) {
            j = url.lastIndexOf(')');
            if (j >= 0) {
                url = url.substring(i + 1, j);
            } else {
                url = url.substring(i + 1);
            }
        } else {
            i = url.indexOf("@");
            if (i >= 0) {
                username = url.substring(0, i);
                j = username.indexOf(":");
                if (j >= 0) {
                    password = username.substring(j + 1);
                    username = username.substring(0, j);
                }
                url = url.substring(i + 1);
            }
            String[] values = url.split(":");
            if (values.length == 2) {
                // 排除zookeeper://192.168.1.2:2181,192.168.1.3:2181
                port = Integer.parseInt(values[1]);
                url = values[0];
            }
        }
        if (!url.isEmpty()) {
            host = url;
        }
        return new URL(protocol, username, password, host, port, path, parameters);
    }

    public static String encode(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        try {
            return URLEncoder.encode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static String decode(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        try {
            return URLDecoder.decode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public String getProtocol() {
        return protocol;
    }

    public URL setProtocol(String protocol) {
        return new URL(protocol, username, password, host, port, path, getParameters());
    }

    public String getAddress() {
        return this.port <= 0 ? this.host : this.host + ":" + this.port;
    }

    public String getHost() {
        return host;
    }

    public URL setHost(String host) {
        return new URL(protocol, username, password, host, port, path, getParameters());
    }

    public int getPort() {
        return port;
    }

    public URL setPort(int port) {
        return new URL(protocol, username, password, host, port, path, getParameters());
    }


    public String getAbsolutePath() {
        if (path != null && !path.startsWith("/")) {
            return "/" + path;
        }
        return path;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public String getParameterAndDecoded(String key) {
        return getParameterAndDecoded(key, null);
    }

    public String getParameterAndDecoded(String key, String defaultValue) {
        return decode(getParameter(key, defaultValue));
    }

    public String getParameter(String key) {
        return parameters.get(key);
    }

    public String getParameter(String key, String defaultValue) {
        String value = getParameter(key);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return value;
    }


    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result + ((path == null) ? 0 : path.hashCode());
        result = prime * result + port;
        result = prime * result + ((protocol == null) ? 0 : protocol.hashCode());
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        URL other = (URL) obj;
        if (host == null) {
            if (other.host != null) {
                return false;
            }
        } else if (!host.equals(other.host)) {
            return false;
        }
        if (path == null) {
            if (other.path != null) {
                return false;
            }
        } else if (!path.equals(other.path)) {
            return false;
        }
        if (port != other.port) {
            return false;
        }
        if (protocol == null) {
            if (other.protocol != null) {
                return false;
            }
        } else if (!protocol.equals(other.protocol)) {
            return false;
        }
        return true;
    }
}
