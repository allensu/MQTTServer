/*******************************************************************************
 * Copyright 2011 Albin Theander
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package MQTTLib;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import MQTTLib.FormatUtil;

public class ConnectMessage extends Message {

	private static final String PROTOCOL_ID = "MQIsdp";
	private static final byte PROTOCOL_VERSION = 3;
	private static int CONNECT_HEADER_SIZE = 12;

	private final String clientId;
	private final int keepAlive;
	private String username;
	private String password;
	private boolean cleanSession;
	private String willTopic;
	private String will;
	private QoS willQoS = QoS.AT_MOST_ONCE;
	private boolean retainWill = false;

	public ConnectMessage(String clientId, boolean cleanSession, int keepAlive) {
		super(Type.CONNECT);
		if (clientId == null || clientId.length() > 23) {
			throw new IllegalArgumentException(
					"Client id cannot be null and must be at most 23 characters long: "
							+ clientId);
		}
		this.clientId = clientId;
		this.cleanSession = cleanSession;
		this.keepAlive = keepAlive;
	}

	@Override
	protected int messageLength() {
		int payloadSize = FormatUtil.toMQttString(clientId).length;
		payloadSize += FormatUtil.toMQttString(willTopic).length;
		payloadSize += FormatUtil.toMQttString(will).length;
		payloadSize += FormatUtil.toMQttString(username).length;
		payloadSize += FormatUtil.toMQttString(password).length;
		return payloadSize + CONNECT_HEADER_SIZE;
	}

	@Override
	protected void writeMessage(OutputStream out) throws IOException {
		DataOutputStream dos = new DataOutputStream(out);
		dos.writeUTF(PROTOCOL_ID);
		dos.write(PROTOCOL_VERSION);
		int flags = cleanSession ? 2 : 0;
		flags |= (will == null) ? 0 : 0x04;
		flags |= willQoS.val << 3;
		flags |= retainWill ? 0x20 : 0;
		flags |= (password == null) ? 0 : 0x40;
		flags |= (username == null) ? 0 : 0x80;
		dos.write((byte) flags);
		dos.writeChar(keepAlive);

		dos.writeUTF(clientId);
		if (will != null) {
			dos.writeUTF(willTopic);
			dos.writeUTF(will);
		}
		if (username != null) {
			dos.writeUTF(username);
		}
		if (password != null) {
			dos.writeUTF(password);
		}
		dos.flush();
	}
	
	public void setCredentials(String username, String password) {
		this.username = username;
		this.password = password;
		
	}

	public void setWill(String willTopic, String will) {
		this.willTopic = willTopic;
		this.will = will;
	}

	public void setWill(String willTopic, String will, QoS willQoS,
			boolean retainWill) {
		this.willTopic = willTopic;
		this.will = will;
		this.willQoS = willQoS;
		this.retainWill = retainWill;

	}
	
	@Override
	public void setDup(boolean dup) {
		throw new UnsupportedOperationException("CONNECT messages don't use the DUP flag.");
	}
	
	@Override
	public void setRetained(boolean retain) {
		throw new UnsupportedOperationException("CONNECT messages don't use the RETAIN flag.");
	}
	
	@Override
	public void setQos(QoS qos) {
		throw new UnsupportedOperationException("CONNECT messages don't use the QoS flags.");
	}

}
