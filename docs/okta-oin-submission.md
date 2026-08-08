# Okta Integration Network (OIN) Submission Guide

This document contains the technical specifications and materials needed for submitting Reiver to the Okta Integration Network.

---

## Application Overview

| Field | Value |
|-------|-------|
| **Application Name** | Reiver |
| **Description** | Error monitoring and observability platform for modern applications |
| **Category** | Developer Tools / Monitoring |
| **Website** | https://reiver.io |
| **Support URL** | https://docs.reiver.io |
| **Support Email** | support@reiver.io |

---

## Integration Capabilities

### Single Sign-On (SSO)

| Capability | Supported |
|------------|-----------|
| **Protocol** | OIDC (OpenID Connect) |
| **Grant Types** | Authorization Code |
| **PKCE** | Supported |
| **Sign-in Redirect** | `https://{customer-domain}/api/sso/callback/okta` |
| **Sign-out Redirect** | `https://{customer-domain}/logout` |
| **Scopes Required** | `openid`, `profile`, `email` |

### SCIM Provisioning

| Capability | Supported |
|------------|-----------|
| **SCIM Version** | 2.0 |
| **Base URL** | `https://{customer-domain}/scim/v2` |
| **Authentication** | Bearer Token |
| **Create Users** | ✅ |
| **Update User Attributes** | ✅ |
| **Deactivate Users** | ✅ |
| **Delete Users** | ✅ (soft delete) |
| **Import Users** | ✅ |
| **Import Groups** | ✅ |
| **Push Groups** | ✅ |
| **Group Push** | ✅ |

---

## SCIM Endpoints

### Discovery Endpoints

```
GET /scim/v2/ServiceProviderConfig
GET /scim/v2/Schemas
GET /scim/v2/ResourceTypes
```

### User Endpoints

```
GET    /scim/v2/Users
POST   /scim/v2/Users
GET    /scim/v2/Users/{id}
PUT    /scim/v2/Users/{id}
PATCH  /scim/v2/Users/{id}
DELETE /scim/v2/Users/{id}
```

### Group Endpoints

```
GET    /scim/v2/Groups
POST   /scim/v2/Groups
GET    /scim/v2/Groups/{id}
PUT    /scim/v2/Groups/{id}
PATCH  /scim/v2/Groups/{id}
DELETE /scim/v2/Groups/{id}
```

---

## Attribute Mappings

### User Attributes

| Okta Attribute | SCIM Attribute | Required | Description |
|----------------|----------------|----------|-------------|
| `login` | `userName` | Yes | Unique username (email) |
| `email` | `emails[type eq "work"].value` | Yes | Primary email address |
| `firstName` | `name.givenName` | No | First name |
| `lastName` | `name.familyName` | No | Last name |
| `displayName` | `displayName` | No | Display name |
| `userStatus` | `active` | No | Account status |

### Group Attributes

| Okta Attribute | SCIM Attribute | Required | Description |
|----------------|----------------|----------|-------------|
| `name` | `displayName` | Yes | Group name |
| `members` | `members` | No | Group members |

---

## SCIM Schema

### User Schema

```json
{
  "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
  "id": "string",
  "externalId": "string",
  "userName": "string",
  "name": {
    "formatted": "string",
    "familyName": "string",
    "givenName": "string"
  },
  "displayName": "string",
  "emails": [
    {
      "value": "string",
      "type": "work",
      "primary": true
    }
  ],
  "active": true,
  "groups": [
    {
      "value": "string",
      "display": "string"
    }
  ],
  "meta": {
    "resourceType": "User",
    "created": "ISO8601",
    "lastModified": "ISO8601",
    "location": "string"
  }
}
```

### Group Schema

```json
{
  "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
  "id": "string",
  "displayName": "string",
  "members": [
    {
      "value": "string",
      "display": "string"
    }
  ],
  "meta": {
    "resourceType": "Group",
    "created": "ISO8601",
    "lastModified": "ISO8601",
    "location": "string"
  }
}
```

---

## Error Responses

### SCIM Error Format

```json
{
  "schemas": ["urn:ietf:params:scim:api:messages:2.0:Error"],
  "status": "400",
  "detail": "Description of the error",
  "scimType": "invalidValue"
}
```

### HTTP Status Codes

| Code | Description |
|------|-------------|
| 200 | Success |
| 201 | Created |
| 204 | No Content (successful delete) |
| 400 | Bad Request |
| 401 | Unauthorized (invalid token) |
| 404 | Not Found |
| 409 | Conflict (duplicate) |
| 500 | Internal Server Error |

---

## Test Credentials

For OIN testing, provide a test environment:

| Field | Value |
|-------|-------|
| Test URL | `https://test.reiver.io` |
| Test SCIM Base URL | `https://test.reiver.io/scim/v2` |
| Test Account Email | `okta-test@reiver.io` |
| Test Account Password | (provided securely) |
| SCIM Bearer Token | (provided securely) |

---

## Configuration Steps for Customers

### Quick Setup (5 minutes)

1. In Okta: Add Reiver from app catalog
2. In Reiver: Copy Client ID/Secret from Okta app
3. In Reiver: Configure SSO with Okta domain
4. In Okta: Enable SCIM provisioning
5. In Okta: Assign users/groups to Reiver app

### Detailed Setup

See: [Okta Integration Guide](./okta-integration-guide.md)

---

## Security & Compliance

| Requirement | Status |
|-------------|--------|
| HTTPS Required | ✅ |
| Bearer Token Auth | ✅ |
| Token Hashing | SHA-256 |
| Data Encryption | AES-256 at rest |
| SOC 2 | In progress |
| GDPR Compliant | ✅ |

---

## Rate Limits

| Endpoint | Limit |
|----------|-------|
| SCIM User operations | 100 req/min |
| SCIM Group operations | 100 req/min |
| SSO Login | 60 req/min per user |

---

## Changelog

| Version | Date | Changes |
|---------|------|---------|
| 1.0.0 | 2026-01 | Initial SCIM 2.0 + OIDC support |

---

## Contact

| Type | Contact |
|------|---------|
| Integration Support | integrations@reiver.io |
| Security Issues | security@reiver.io |
| General Support | support@reiver.io |

---

## OIN Submission Checklist

- [ ] SCIM 2.0 endpoints implemented and tested
- [ ] OIDC SSO implemented and tested
- [ ] Integration guide documentation complete
- [ ] Logo assets prepared (PNG, 256x256)
- [ ] Test environment provisioned
- [ ] Support contact designated
- [ ] Submit at https://developer.okta.com/docs/guides/submit-app/
