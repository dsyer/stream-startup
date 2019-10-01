/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.demo;

import java.nio.ByteBuffer;

import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.util.DigestUtils;

/**
 * @author Dave Syer
 *
 */
@Component
public class DefaultKeyExtractor implements KeyExtractor {

	@Override
	public byte[] extract(Message<byte[]> message) {
		Object object = message.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY);
		if (object instanceof byte[]) {
			return (byte[]) object;
		}
		if (object instanceof Long) {
			return getBytes((Long) object);
		}
		if (object !=null) {
			throw new IllegalArgumentException("Message key type not supported (must be byte[] or long): " + (object == null ? null : object.getClass()));
		}
		return DigestUtils.md5Digest(message.getPayload());
	}

	static byte[] getBytes(Long offset) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.putLong(offset);
		return buffer.array();
	}

}
