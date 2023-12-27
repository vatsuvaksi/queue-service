package com.example;

public class Message {
  /** How many times this message has been delivered. */
  private int attempts;

  /** Visible from time */
  private long visibleFrom;

  /** An identifier associated with the act of receiving the message. */
  private String receiptId;

  private String msgBody;
  private int priority;

  Message(String msgBody) {
    this.msgBody = msgBody;
  }

  Message(String msgBody, String receiptId) {
    this.msgBody = msgBody;
    this.receiptId = receiptId;
  }
  Message(String msgBody , int priority){
    this.msgBody = msgBody;
    this.priority = priority;
  }

  public String getReceiptId() {
    return this.receiptId;
  }

  protected void setReceiptId(String receiptId) {
    this.receiptId = receiptId;
  }

  protected void setVisibleFrom(long visibleFrom) {
    this.visibleFrom = visibleFrom;
  }

  /*
  public boolean isVisible() {
  	return visibleFrom < System.currentTimeMillis();
  }*/

  public boolean isVisibleAt(long instant) {
    return visibleFrom < instant;
  }

  public String getBody() {
    return msgBody;
  }

  protected int getAttempts() {
    return attempts;
  }

  protected void incrementAttempts() {
    this.attempts++;
  }

  protected int getPriority(){
    return this.priority;
  }
}
