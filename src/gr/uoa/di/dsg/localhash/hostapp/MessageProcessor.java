package gr.uoa.di.dsg.localhash.hostapp;

import java.io.IOException;

public interface MessageProcessor {
	void processMessage(Message msg, Node source) throws Exception;
	void processTimeout(int timeoutId, Object extraData) throws Exception;
}
