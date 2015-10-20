/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.ui;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import lombok.extern.slf4j.Slf4j;
import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.server.LensJerseyTest;

import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.session.SessionService;
import org.apache.lens.server.session.HiveSessionService;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Set;

/**
 * The Class TestSessionUIResource.
 */
@Test(groups = "unit-test")
@Slf4j
public class TestSessionUIResource extends LensJerseyTest {

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#setUp()
   */
  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#tearDown()
   */
  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#configure()
   */
  @Override
  protected Application configure() {
    return new UIApp();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#configureClient(org.glassfish.jersey.client.ClientConfig)
   */
  @Override
  protected void configureClient(ClientConfig config) {
    config.register(MultiPartFeature.class);
  }

  private FormDataMultiPart getMultiFormData(String username, String password) {
    final FormDataMultiPart mp = new FormDataMultiPart();

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("username").build(), username));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(), password));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
        new LensConf(), MediaType.APPLICATION_XML_TYPE));
    return mp;
  }

  /**
   * Test ui session
   */
  @Test
  public void testUISession() throws LensException {
    final WebTarget target = target().path("uisession");
    FormDataMultiPart mp = getMultiFormData("UITest", "UITest");

    LensSessionHandle lensSessionHandle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), LensSessionHandle.class);
    Assert.assertTrue(lensSessionHandle != null);

    Response deleteResponse = target.path(lensSessionHandle.getPublicId().toString()).request().delete();
    Assert.assertEquals(deleteResponse.getStatus(), 200);
    HiveSessionService sessionService = LensServices.get().getService(SessionService.NAME);
    sessionService.closeSession(lensSessionHandle);
  }

  @Test
  public void testJsonResponsesFromServer() {
    final WebTarget target = target().path("uisession");
    FormDataMultiPart mp = getMultiFormData("UITest", "UITest");

    Response response = target.request().accept(MediaType.APPLICATION_JSON).
        post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));
    Assert.assertEquals(response.getStatus(), 200);
    Assert.assertEquals(response.getMediaType().toString(), "application/json");
    //closeSessions();
  }

  @Test
  public void testXMLResponsesFromServer() {
    final WebTarget target = target().path("uisession");
    FormDataMultiPart mp = getMultiFormData("UITest", "UITest");

    Response response = target.request().accept(MediaType.APPLICATION_XML).
        post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE));
    Assert.assertEquals(response.getStatus(), 200);
    Assert.assertEquals(response.getMediaType().toString(), "application/xml");
    //closeSessions();
  }

  private void closeSessions() {
    HiveSessionService service = LensServices.get().getService(SessionService.NAME);
    Set<LensSessionHandle> sessionHandleSet = service.getSessionHandleToUserMap().keySet();
    for (LensSessionHandle session : sessionHandleSet) {
      try {
        service.closeSession(session);
      } catch (LensException e) {
        log.warn("Got Exception while closing {} session" + session);
      }
    }
  }
}
