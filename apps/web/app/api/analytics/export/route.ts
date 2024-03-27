import { getAnalytics } from "@/lib/analytics";
import { DubApiError } from "@/lib/api/errors";
import { withAuth } from "@/lib/auth";
import { getDomainViaEdge } from "@/lib/planetscale";
import prisma from "@/lib/prisma";
import { getAnalyticsQuerySchema } from "@/lib/zod/schemas/analytics";
import { linkConstructor } from "@dub/utils";
import { json2csv } from "json-2-csv";
import JSZip from "jszip";

const exportableEndpoints = [
  "timeseries",
  "country",
  "top_urls",
  "device",
  "referer",
  "city",
  "browser",
  "os",
  "top_links",
];

const convertToCSV = (data) => {
  return json2csv(data, {
    parseValue(fieldValue, defaultParser) {
      if (fieldValue instanceof Date) {
        return fieldValue.toISOString();
      }
      return defaultParser(fieldValue);
    },
  });
};

export const GET = withAuth(
  async ({ searchParams, workspace, link }) => {
    const parsedParams = getAnalyticsQuerySchema.parse(searchParams);
    const { domain, key, interval } = parsedParams;

    if (
      workspace?.plan === "free" &&
      (interval === "all" || interval === "90d")
    ) {
      throw new DubApiError({
        code: "forbidden",
        message: "Require higher plan",
      });
    }

    const linkId = link
      ? link.id
      : domain && key === "_root"
        ? await getDomainViaEdge(domain).then((d) => d?.id)
        : null;

    const zip = new JSZip();

    for (const endpoint of exportableEndpoints) {
      let response;
      if (endpoint === "top_links") {
        const data = await getAnalytics({
          workspaceId: workspace.id,
          endpoint: "top_links",
          ...parsedParams,
        });

        if (!data || data.length === 0) continue;

        const [links, domains] = await Promise.all([
          prisma.link.findMany({
            where: {
              projectId: workspace.id,
              id: { in: data.map(({ link }) => link) },
            },
            select: { id: true, domain: true, key: true, url: true },
          }),
          prisma.domain.findMany({
            where: {
              projectId: workspace.id,
              id: { in: data.map(({ link }) => link) },
            },
            select: { id: true, slug: true, target: true },
          }),
        ]);

        const allLinks = [
          ...links.map((link) => ({
            linkId: link.id,
            shortLink: linkConstructor({
              domain: link.domain,
              key: link.key,
              pretty: true,
            }),
            url: link.url,
          })),
          ...domains.map((domain) => ({
            linkId: domain.id,
            shortLink: linkConstructor({
              domain: domain.slug,
              pretty: true,
            }),
            url: domain.target || "",
          })),
        ];

        const topLinks = data.map((d) => ({
          ...allLinks.find((l) => l.linkId === d.link),
          clicks: d.clicks,
        }));

        const csvData = convertToCSV(topLinks);
        zip.file(`${endpoint}.csv`, csvData);
      } else {
        response = await getAnalytics({
          workspaceId: workspace.id,
          ...(linkId && { linkId }),
          endpoint,
          ...parsedParams,
        });

        if (!response || response.length === 0) continue;

        const csvData = convertToCSV(response);
        zip.file(`${endpoint}.csv`, csvData);
      }
    }

    const zipData = await zip.generateAsync({ type: "nodebuffer" });

    return new Response(zipData, {
      headers: {
        "Content-Type": "application/zip",
        "Content-Disposition": "attachment; filename=analytics_export.zip",
      },
    });
  },
  {
    needNotExceededClicks: true,
  },
);
