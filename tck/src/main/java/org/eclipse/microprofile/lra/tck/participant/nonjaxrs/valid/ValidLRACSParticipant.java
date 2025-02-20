/*
 *******************************************************************************
 * Copyright (c) 2019 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.eclipse.microprofile.lra.tck.participant.nonjaxrs.valid;

import static org.eclipse.microprofile.lra.tck.participant.api.ParticipatingTckResource.ACCEPT_PATH;
import static org.eclipse.microprofile.lra.tck.participant.api.ParticipatingTckResource.RECOVERY_PARAM;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.eclipse.microprofile.lra.annotation.Compensate;
import org.eclipse.microprofile.lra.annotation.Complete;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;
import org.eclipse.microprofile.lra.annotation.Status;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;
import org.eclipse.microprofile.lra.tck.service.LRAMetricService;
import org.eclipse.microprofile.lra.tck.service.LRAMetricType;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Response;

/**
 * Valid participant resource containing async non-JAX-RS participant methods with {@link CompletionStage} return types
 */
@ApplicationScoped
@Path(ValidLRACSParticipant.ROOT_PATH)
public class ValidLRACSParticipant {

    public static final String ROOT_PATH = "valid-cs-participant1";
    public static final String ENLIST_WITH_COMPLETE = "enlist-complete";
    public static final String ENLIST_WITH_COMPENSATE = "enlist-compensate";

    private int recoveryPasses;

    @Inject
    private LRAMetricService lraMetricService;

    @GET
    @Path(ENLIST_WITH_COMPLETE)
    @LRA(value = LRA.Type.REQUIRED)
    public Response enlistWithComplete(@HeaderParam(LRA.LRA_HTTP_CONTEXT_HEADER) URI lraId) {
        return Response.ok(lraId).build();
    }

    @GET
    @Path(ENLIST_WITH_COMPENSATE)
    @LRA(value = LRA.Type.REQUIRED, cancelOn = Response.Status.INTERNAL_SERVER_ERROR)
    public Response enlistWithCompensate(@HeaderParam(LRA.LRA_HTTP_CONTEXT_HEADER) URI lraId) {
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(lraId).build();
    }

    @Compensate
    public CompletionStage<Void> compensate(URI lraId) {
        assert lraId != null;

        Executor delayed = CompletableFuture.delayedExecutor(1L, TimeUnit.SECONDS);
        return CompletableFuture.runAsync(
                () -> {
                    lraMetricService.incrementMetric(LRAMetricType.Compensated, lraId, ValidLRACSParticipant.class);
                }, delayed);
    }

    @Complete
    public CompletionStage<Response> complete(URI lraId) {
        assert lraId != null;
        Executor delayed = CompletableFuture.delayedExecutor(1L, TimeUnit.SECONDS);
        return CompletableFuture.supplyAsync(() -> {
            lraMetricService.incrementMetric(LRAMetricType.Completed, lraId, ValidLRACSParticipant.class);
            return Response.accepted().build(); // Completing
        }, delayed);
    }

    /*
     * The @Status method, if present, MUST report the progress of the compensation. Similarly, if the resource cannot
     * perform a completion activity immediately.
     */
    @Status
    public CompletionStage<ParticipantStatus> status(URI lraId) {
        assert lraId != null;

        return CompletableFuture.supplyAsync(() -> {
            lraMetricService.incrementMetric(LRAMetricType.Status, lraId, ValidLRACSParticipant.class);

            int completed = lraMetricService.getMetric(LRAMetricType.Completed, lraId, ValidLRACSParticipant.class);

            int compensated = lraMetricService.getMetric(LRAMetricType.Compensated, lraId, ValidLRACSParticipant.class);
            // setting as default Completing, but it could also be Active or Compensating
            // as default it is not Ready (nor completed or compensated)
            ParticipantStatus response = ParticipantStatus.Completing;
            if (completed > 0) {
                response = ParticipantStatus.Completed;
            }
            if (compensated > 0) {
                response = ParticipantStatus.Compensated;
            }
            return response;
        });
    }

    @PUT
    @Path(ACCEPT_PATH)
    @LRA(value = LRA.Type.REQUIRES_NEW)
    public Response acceptLRA(@QueryParam(RECOVERY_PARAM) @DefaultValue("0") Integer recoveryPasses) {
        this.recoveryPasses = recoveryPasses;

        return Response.ok().build();
    }

    @GET
    @Path(ACCEPT_PATH)
    public Response getAcceptLRA() {
        return Response.ok(this.recoveryPasses).build();
    }

}
