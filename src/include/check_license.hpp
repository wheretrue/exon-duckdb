#pragma once

#include <cstdlib>
#include <string>

namespace wtt01
{
    class LicenseCheck
    {
    public:
        enum class LicenseStatus
        {
            ACTIVE,
            INACTIVE,
            INVALID_ENV_VAR_CONFIGURATION,
            SERVER_CONNECTION_ERROR,
            UNEXPECTED_HTTP_STATUS_ERROR,
            LICENSE_NOT_FOUND,
        };

    public:
        static std::string GetLicenseStatusString(LicenseStatus license_status);
        static void ValidateLicense();

    private:
        static LicenseStatus GetLicenseStatus();
    };

};
